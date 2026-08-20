use crate::{
    config::Config,
    domain::{AccountSnapshot, Direction, OrderIntent},
};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillReport {
    pub fill_id: String,
    pub quantity: i64,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    pub final_fill: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionReport {
    Rejected {
        order_id: String,
        reason: String,
    },
    Accepted {
        order_id: String,
        fills: Vec<FillReport>,
    },
    Cancelled {
        order_id: String,
        reason: String,
    },
}

pub trait ExecutionGateway {
    fn roll_trading_day(&mut self, date: NaiveDate) -> Result<()>;
    fn submit(&mut self, intent: &OrderIntent) -> Result<ExecutionReport>;
    fn cancel(&mut self, intent: &OrderIntent, reason: &str) -> Result<ExecutionReport>;
    fn report_for_intent(&self, intent_id: &str) -> Result<Option<ExecutionReport>>;
    fn account_snapshot(&self) -> AccountSnapshot;
    fn synchronize_snapshot(&mut self, snapshot: AccountSnapshot);
}

pub struct PaperExecutionGateway {
    config: Config,
    run_id: String,
    conn: Connection,
    seed: u64,
    reject_bps: u16,
    partial_bps: u16,
    hold_open_bps: u16,
    latency_ms: u64,
    slippage_rate: Decimal,
    price_scale: u32,
    snapshot: AccountSnapshot,
    current_trade_date: Option<NaiveDate>,
}

impl PaperExecutionGateway {
    pub fn new(config: &Config, run_id: &str, snapshot: AccountSnapshot) -> Result<Self> {
        Self::open(config, run_id, snapshot, true)
    }

    pub(crate) fn open_existing(
        config: &Config,
        run_id: &str,
        snapshot: AccountSnapshot,
    ) -> Result<Self> {
        Self::open(config, run_id, snapshot, false)
    }

    fn open(
        config: &Config,
        run_id: &str,
        snapshot: AccountSnapshot,
        create_if_missing: bool,
    ) -> Result<Self> {
        let conn = Connection::open(&config.database)
            .context("failed to open independent Paper Broker ledger")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let stored: Option<String> = conn
            .query_row(
                "SELECT snapshot_json FROM paper_accounts WHERE run_id=?1",
                [run_id],
                |row| row.get(0),
            )
            .optional()?;
        let snapshot = if let Some(stored) = stored {
            serde_json::from_str(&stored).context("invalid Paper Broker account snapshot")?
        } else if create_if_missing {
            conn.execute(
                "INSERT INTO paper_accounts(run_id,snapshot_json,updated_at) VALUES(?1,?2,?3)",
                params![
                    run_id,
                    serde_json::to_string(&snapshot)?,
                    chrono::Utc::now().naive_utc().to_string()
                ],
            )?;
            snapshot
        } else {
            anyhow::bail!("independent Paper Broker account is missing")
        };
        let current_trade_date = conn
            .query_row(
                "SELECT trade_date FROM paper_trade_dates WHERE run_id=?1",
                [run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|date| NaiveDate::parse_from_str(&date, "%Y-%m-%d"))
            .transpose()?;
        Ok(Self {
            config: config.clone(),
            run_id: run_id.to_owned(),
            conn,
            seed: config.paper.seed,
            reject_bps: config.paper.reject_probability_bps,
            partial_bps: config.paper.partial_fill_bps,
            hold_open_bps: config.paper.hold_open_probability_bps,
            latency_ms: config.paper.latency_ms,
            slippage_rate: config.slippage_rate,
            price_scale: config.price_scale,
            snapshot,
            current_trade_date,
        })
    }

    fn stable_id(kind: &str, intent_id: &str, part: usize) -> String {
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("paper:{kind}:{intent_id}:{part}").as_bytes(),
        )
        .to_string()
    }

    fn stable_draw_bps(&self, intent_id: &str, purpose: &str) -> u16 {
        let digest = Sha256::digest(format!("{}:{intent_id}:{purpose}", self.seed));
        u16::from_be_bytes([digest[0], digest[1]]) % 10_000
    }

    fn persist_report(&mut self, intent: &OrderIntent, report: &ExecutionReport) -> Result<()> {
        let mut next = self.snapshot.clone();
        if let ExecutionReport::Accepted { order_id, fills } = report {
            if fills.is_empty() {
                if !next.open_order_ids.contains(order_id) {
                    next.open_order_ids.push(order_id.clone());
                    next.open_order_ids.sort();
                }
                if intent.direction == Direction::Sell {
                    next.position.frozen_sell = next
                        .position
                        .frozen_sell
                        .checked_add(intent.quantity)
                        .context("NUMERIC_RANGE_VIOLATION: Paper frozen sell overflow")?;
                }
            } else {
                next.open_order_ids.retain(|id| id != order_id);
            }
            let mut filled_notional = Decimal::ZERO;
            let mut commission_charged = Decimal::ZERO;
            for fill in fills {
                let notional = fill
                    .price
                    .checked_mul(Decimal::from(fill.quantity))
                    .context("NUMERIC_RANGE_VIOLATION: Paper fill notional overflow")?;
                let prior_notional = filled_notional;
                filled_notional = filled_notional
                    .checked_add(notional)
                    .context("NUMERIC_RANGE_VIOLATION: Paper cumulative notional overflow")?;
                let (commission, tax) =
                    crate::profit::checked_actual_order_fill_charges_with_policy(
                        &crate::domain::ProfitGuardPolicy::from(&self.config),
                        intent.direction,
                        prior_notional,
                        commission_charged,
                        fill.price,
                        fill.quantity,
                    )
                    .context("NUMERIC_RANGE_VIOLATION: Paper fill charges overflow")?;
                commission_charged = commission_charged
                    .checked_add(commission)
                    .context("NUMERIC_RANGE_VIOLATION: Paper commission overflow")?;
                let fees = commission
                    .checked_add(tax)
                    .context("NUMERIC_RANGE_VIOLATION: Paper fees overflow")?;
                match intent.direction {
                    Direction::Buy => {
                        let debit = notional
                            .checked_add(fees)
                            .context("NUMERIC_RANGE_VIOLATION: Paper BUY debit overflow")?;
                        next.cash.available = next
                            .cash
                            .available
                            .checked_sub(debit)
                            .context("NUMERIC_RANGE_VIOLATION: Paper BUY cash overflow")?;
                        next.position.total = next
                            .position
                            .total
                            .checked_add(fill.quantity)
                            .context("NUMERIC_RANGE_VIOLATION: Paper BUY position overflow")?;
                        next.position.today_bought = next
                            .position
                            .today_bought
                            .checked_add(fill.quantity)
                            .context("NUMERIC_RANGE_VIOLATION: Paper BUY T+1 overflow")?;
                    }
                    Direction::Sell => {
                        let credit = notional
                            .checked_sub(fees)
                            .context("NUMERIC_RANGE_VIOLATION: Paper SELL credit overflow")?;
                        next.cash.available = next
                            .cash
                            .available
                            .checked_add(credit)
                            .context("NUMERIC_RANGE_VIOLATION: Paper SELL cash overflow")?;
                        next.position.total =
                            next.position.total.checked_sub(fill.quantity).context(
                                "NUMERIC_RANGE_VIOLATION: Paper SELL position underflow",
                            )?;
                        next.position.sellable =
                            next.position.sellable.checked_sub(fill.quantity).context(
                                "NUMERIC_RANGE_VIOLATION: Paper SELL sellable underflow",
                            )?;
                    }
                }
                next.cash.total_fees = next
                    .cash
                    .total_fees
                    .checked_add(fees)
                    .context("NUMERIC_RANGE_VIOLATION: Paper cumulative fees overflow")?;
                next.cumulative_filled_quantity = next
                    .cumulative_filled_quantity
                    .checked_add(fill.quantity)
                    .context("NUMERIC_RANGE_VIOLATION: Paper cumulative fill overflow")?;
            }
        } else if let ExecutionReport::Cancelled { order_id, .. } = report {
            next.open_order_ids.retain(|id| id != order_id);
        }
        let order_id = match report {
            ExecutionReport::Rejected { order_id, .. }
            | ExecutionReport::Accepted { order_id, .. }
            | ExecutionReport::Cancelled { order_id, .. } => order_id,
        };
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO paper_orders(run_id,intent_id,order_id,report_json,created_at) VALUES(?1,?2,?3,?4,?5)",
            params![self.run_id, intent.intent_id, order_id, serde_json::to_string(report)?, chrono::Utc::now().naive_utc().to_string()],
        )?;
        tx.execute(
            "UPDATE paper_accounts SET snapshot_json=?2,updated_at=?3 WHERE run_id=?1",
            params![
                self.run_id,
                serde_json::to_string(&next)?,
                chrono::Utc::now().naive_utc().to_string()
            ],
        )?;
        tx.commit()?;
        self.snapshot = next;
        Ok(())
    }

    fn sell_report_obeys_no_loss_guard(&self, intent: &OrderIntent, fills: &[FillReport]) -> bool {
        if intent.direction != Direction::Sell
            || intent.profit_guard_version != crate::profit::PROFIT_GUARD_VERSION
            || intent.target_lot_allocations.is_empty()
        {
            return intent.direction != Direction::Sell;
        }
        let Some(policy) = intent.profit_guard_policy.as_ref() else {
            return false;
        };
        if policy != &crate::domain::ProfitGuardPolicy::from(&self.config) {
            return false;
        }
        if fills.is_empty() {
            return intent
                .target_lot_allocations
                .iter()
                .all(|allocation| allocation.worst_case_profit >= Decimal::ZERO);
        }
        let mut filled_quantity = 0_i64;
        let mut prior_notional = Decimal::ZERO;
        let mut prior_commission = Decimal::ZERO;
        for fill in fills {
            let Some(fill_allocations) = crate::profit::canonical_fill_allocations(
                policy,
                &intent.target_lot_allocations,
                filled_quantity,
                fill.quantity,
                intent.limit_price,
            ) else {
                return false;
            };
            if fill_allocations
                .iter()
                .any(|allocation| fill.price < allocation.worst_fill_price)
            {
                return false;
            }
            let Some((commission, tax)) =
                crate::profit::checked_actual_order_fill_charges_with_policy(
                    &crate::domain::ProfitGuardPolicy::from(&self.config),
                    Direction::Sell,
                    prior_notional,
                    prior_commission,
                    fill.price,
                    fill.quantity,
                )
            else {
                return false;
            };
            let Some(total_fees) = commission.checked_add(tax) else {
                return false;
            };
            let Some(fees) =
                crate::profit::allocate_actual_fill_fees(&fill_allocations, total_fees)
            else {
                return false;
            };
            if fill_allocations.iter().zip(fees).any(|(allocation, fee)| {
                fill.price
                    .checked_mul(Decimal::from(allocation.quantity))
                    .and_then(|value| value.checked_sub(allocation.cost_basis))
                    .and_then(|value| value.checked_sub(fee))
                    .is_none_or(|pnl| pnl < Decimal::ZERO)
            }) {
                return false;
            }
            let Some(fill_notional) = fill.price.checked_mul(Decimal::from(fill.quantity)) else {
                return false;
            };
            let Some(next_notional) = prior_notional.checked_add(fill_notional) else {
                return false;
            };
            let Some(next_commission) = prior_commission.checked_add(commission) else {
                return false;
            };
            let Some(next_quantity) = filled_quantity.checked_add(fill.quantity) else {
                return false;
            };
            prior_notional = next_notional;
            prior_commission = next_commission;
            filled_quantity = next_quantity;
        }
        filled_quantity == intent.quantity
    }
}

impl ExecutionGateway for PaperExecutionGateway {
    fn roll_trading_day(&mut self, date: NaiveDate) -> Result<()> {
        if self.current_trade_date == Some(date) {
            return Ok(());
        }
        let mut next = self.snapshot.clone();
        if self.current_trade_date.is_some() {
            next.position.sellable = next
                .position
                .sellable
                .checked_add(next.position.today_bought)
                .context("NUMERIC_RANGE_VIOLATION: Paper day-roll position overflow")?;
            next.position.today_bought = 0;
        }
        let now = chrono::Utc::now().naive_utc().to_string();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE paper_accounts SET snapshot_json=?2,updated_at=?3 WHERE run_id=?1",
            params![self.run_id, serde_json::to_string(&next)?, now],
        )?;
        tx.execute(
            "INSERT INTO paper_trade_dates(run_id,trade_date,updated_at) VALUES(?1,?2,?3)
             ON CONFLICT(run_id) DO UPDATE SET trade_date=excluded.trade_date,updated_at=excluded.updated_at",
            params![self.run_id, date.to_string(), now],
        )?;
        tx.commit()?;
        self.snapshot = next;
        self.current_trade_date = Some(date);
        Ok(())
    }

    fn submit(&mut self, intent: &OrderIntent) -> Result<ExecutionReport> {
        self.roll_trading_day(intent.created_at.date())?;
        if intent.profit_guard_policy.as_ref()
            != Some(&crate::domain::ProfitGuardPolicy::from(&self.config))
        {
            anyhow::bail!("order fee policy does not match the paper account configuration");
        }
        if let Some(report) = self.report_for_intent(&intent.intent_id)? {
            return Ok(report);
        }
        if self.latency_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.latency_ms));
        }
        let order_id = Self::stable_id("order", &intent.intent_id, 0);
        if self.stable_draw_bps(&intent.intent_id, "reject") < self.reject_bps {
            let report = ExecutionReport::Rejected {
                order_id,
                reason: "PAPER_CONFIGURED_REJECTION".to_owned(),
            };
            self.persist_report(intent, &report)?;
            return Ok(report);
        }
        let slip = match intent.direction {
            Direction::Buy => Decimal::ONE + self.slippage_rate,
            Direction::Sell => Decimal::ONE - self.slippage_rate,
        };
        let rounding = if intent.direction == Direction::Buy {
            RoundingStrategy::ToPositiveInfinity
        } else {
            RoundingStrategy::ToNegativeInfinity
        };
        let price = intent
            .limit_price
            .checked_mul(slip)
            .context("NUMERIC_RANGE_VIOLATION: Paper adverse fill price overflow")?
            .round_dp_with_strategy(self.price_scale, rounding);
        let partial = intent.quantity >= 2 * self.config.lot_size
            && self.stable_draw_bps(&intent.intent_id, "partial") < self.partial_bps;
        let hold_open = intent.direction == Direction::Sell
            && self.stable_draw_bps(&intent.intent_id, "hold-open") < self.hold_open_bps;
        let fills = if hold_open {
            Vec::new()
        } else if partial {
            let first = (intent.quantity / 2 / self.config.lot_size) * self.config.lot_size;
            vec![
                FillReport {
                    fill_id: Self::stable_id("fill", &intent.intent_id, 1),
                    quantity: first,
                    price,
                    final_fill: false,
                },
                FillReport {
                    fill_id: Self::stable_id("fill", &intent.intent_id, 2),
                    quantity: intent.quantity - first,
                    price,
                    final_fill: true,
                },
            ]
        } else {
            vec![FillReport {
                fill_id: Self::stable_id("fill", &intent.intent_id, 1),
                quantity: intent.quantity,
                price,
                final_fill: true,
            }]
        };
        if !self.sell_report_obeys_no_loss_guard(intent, &fills) {
            let report = ExecutionReport::Rejected {
                order_id,
                reason: "PAPER_NO_LOSS_GUARD_REJECTED".to_owned(),
            };
            self.persist_report(intent, &report)?;
            return Ok(report);
        }
        let report = ExecutionReport::Accepted { order_id, fills };
        self.persist_report(intent, &report)?;
        Ok(report)
    }

    fn cancel(&mut self, intent: &OrderIntent, reason: &str) -> Result<ExecutionReport> {
        if let Some(existing) = self.report_for_intent(&intent.intent_id)? {
            match existing {
                ExecutionReport::Accepted { order_id, fills } if fills.is_empty() => {
                    let report = ExecutionReport::Cancelled {
                        order_id,
                        reason: reason.to_owned(),
                    };
                    let next = {
                        let mut snapshot = self.snapshot.clone();
                        snapshot.open_order_ids.retain(|id| {
                            id != match &report {
                                ExecutionReport::Cancelled { order_id, .. } => order_id,
                                _ => unreachable!(),
                            }
                        });
                        if intent.direction == Direction::Sell {
                            snapshot.position.frozen_sell = snapshot
                                .position
                                .frozen_sell
                                .checked_sub(intent.quantity)
                                .context(
                                    "NUMERIC_RANGE_VIOLATION: Paper cancel frozen sell underflow",
                                )?;
                        }
                        snapshot
                    };
                    let tx = self
                        .conn
                        .transaction_with_behavior(TransactionBehavior::Immediate)?;
                    tx.execute(
                        "UPDATE paper_orders SET report_json=?3 WHERE run_id=?1 AND intent_id=?2",
                        params![
                            self.run_id,
                            intent.intent_id,
                            serde_json::to_string(&report)?
                        ],
                    )?;
                    tx.execute(
                        "UPDATE paper_accounts SET snapshot_json=?2,updated_at=?3 WHERE run_id=?1",
                        params![
                            self.run_id,
                            serde_json::to_string(&next)?,
                            chrono::Utc::now().naive_utc().to_string()
                        ],
                    )?;
                    tx.commit()?;
                    self.snapshot = next;
                    return Ok(report);
                }
                report => return Ok(report),
            }
        }
        self.submit(intent)
    }

    fn report_for_intent(&self, intent_id: &str) -> Result<Option<ExecutionReport>> {
        let report: Option<String> = self
            .conn
            .query_row(
                "SELECT report_json FROM paper_orders WHERE run_id=?1 AND intent_id=?2",
                params![self.run_id, intent_id],
                |row| row.get(0),
            )
            .optional()?;
        report
            .map(|report| serde_json::from_str(&report).context("invalid Paper Broker report"))
            .transpose()
    }

    fn account_snapshot(&self) -> AccountSnapshot {
        self.snapshot.clone()
    }

    fn synchronize_snapshot(&mut self, _snapshot: AccountSnapshot) {
        // Deliberately independent: reconciliation must never overwrite broker state.
    }
}
