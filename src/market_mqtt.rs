use anyhow::{bail, Context, Result};
use rumqttc::v5::{
    mqttbytes::QoS, Client, Event, Incoming, MqttOptions, RecvTimeoutError as MqttRecvTimeoutError,
};
use rumqttc::Transport;
use std::{
    collections::VecDeque,
    fs,
    path::Path,
    sync::mpsc::{self, Receiver, RecvTimeoutError as ChannelRecvTimeoutError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketMqttMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

fn validate_common_wire_contract(
    payload: &[u8],
    qos: QoS,
    retained: bool,
    content_type: Option<&str>,
) -> Result<()> {
    if qos != QoS::AtLeastOnce {
        bail!("market MQTT message must use QoS 1")
    }
    if retained {
        bail!("market data events must not be retained")
    }
    if content_type != Some("application/json") {
        bail!("market MQTT message must declare application/json")
    }
    if payload.is_empty() || payload.len() > 1_048_576 {
        bail!("market MQTT payload size is invalid")
    }
    Ok(())
}

impl MarketMqttMessage {
    pub fn from_committed_wire_contract(
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retained: bool,
        content_type: Option<&str>,
    ) -> Result<Self> {
        validate_common_wire_contract(payload, qos, retained, content_type)?;
        let suffix = topic
            .strip_prefix("gridedge/market-committed/v1/")
            .context("worker received a market event before its PostgreSQL commit")?;
        let mut segments = suffix.split('/');
        let venue = segments
            .next()
            .context("committed market topic lacks venue")?;
        let symbol = segments
            .next()
            .context("committed market topic lacks symbol")?;
        let kind = segments
            .next()
            .context("committed market topic lacks event kind")?;
        if segments.next().is_some()
            || !matches!(venue, "XSHE" | "XSHG")
            || symbol.len() != 6
            || !symbol.bytes().all(|byte| byte.is_ascii_digit())
            || !matches!(kind, "trade" | "status")
        {
            bail!("committed market topic identity is invalid")
        }
        Ok(Self {
            topic: format!("gridedge/market/v1/{venue}/{symbol}/{kind}"),
            payload: payload.to_vec(),
        })
    }

    pub fn from_wire_contract(
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retained: bool,
        content_type: Option<&str>,
    ) -> Result<Self> {
        validate_common_wire_contract(payload, qos, retained, content_type)?;
        Ok(Self {
            topic: topic.to_owned(),
            payload: payload.to_vec(),
        })
    }
}

pub struct MarketMqttClient {
    _client: Client,
    pump_receiver: Receiver<MarketMqttPumpEvent>,
    _pump: JoinHandle<()>,
    subscription_acks: usize,
    pending: VecDeque<MarketMqttMessage>,
}

enum MarketMqttPumpEvent {
    SubscriptionAcknowledged,
    Message(MarketMqttMessage),
    Fatal(String),
}

enum MarketMqttPumpPoll {
    Idle,
    Event(MarketMqttPumpEvent),
}

impl MarketMqttClient {
    #[allow(clippy::too_many_arguments)]
    pub fn connect(
        host: &str,
        port: u16,
        username: &str,
        password_file: &Path,
        ca_file: &Path,
        client_id: &str,
        venue: &str,
        symbol: &str,
    ) -> Result<Self> {
        if host.trim().is_empty() || username.trim().is_empty() || client_id.trim().is_empty() {
            bail!("market MQTT connection identity is incomplete")
        }
        if !matches!(venue, "XSHE" | "XSHG")
            || symbol.len() != 6
            || !symbol.bytes().all(|byte| byte.is_ascii_digit())
        {
            bail!("market MQTT subscription instrument is invalid")
        }
        validate_private_file(password_file)?;
        let password = fs::read_to_string(password_file)
            .with_context(|| format!("failed to read {}", password_file.display()))?;
        let password = password.trim_end_matches(['\r', '\n']);
        if password.is_empty() {
            bail!("market MQTT password is empty")
        }
        let ca =
            fs::read(ca_file).with_context(|| format!("failed to read {}", ca_file.display()))?;
        if ca.is_empty() {
            bail!("market MQTT CA file is empty")
        }

        let mut options = MqttOptions::new(client_id, host, port);
        options
            .set_credentials(username, password)
            .set_transport(Transport::tls(ca, None, None))
            .set_keep_alive(Duration::from_secs(20))
            .set_clean_start(false)
            .set_session_expiry_interval(Some(31_536_000));
        let (client, mut connection) = Client::new(options, 256);
        client.subscribe(
            format!("gridedge/market-committed/v1/{venue}/{symbol}/trade"),
            QoS::AtLeastOnce,
        )?;
        client.subscribe(
            format!("gridedge/market-committed/v1/{venue}/{symbol}/status"),
            QoS::AtLeastOnce,
        )?;
        let (pump_receiver, pump) =
            spawn_market_mqtt_pump(format!("market-mqtt-{venue}-{symbol}"), move || {
                let event = match connection.recv_timeout(Duration::from_secs(1)) {
                    Ok(Ok(event)) => event,
                    Ok(Err(error)) => {
                        return MarketMqttPumpPoll::Event(MarketMqttPumpEvent::Fatal(format!(
                            "market MQTT connection failed: {error}"
                        )))
                    }
                    Err(MqttRecvTimeoutError::Timeout) => return MarketMqttPumpPoll::Idle,
                    Err(MqttRecvTimeoutError::Disconnected) => {
                        return MarketMqttPumpPoll::Event(MarketMqttPumpEvent::Fatal(
                            "market MQTT request channel disconnected".to_owned(),
                        ))
                    }
                };
                match event {
                    Event::Incoming(Incoming::SubAck(suback)) => {
                        match suback.return_codes.as_slice() {
                            [rumqttc::v5::mqttbytes::v5::SubscribeReasonCode::Success(
                                QoS::AtLeastOnce,
                            )] => MarketMqttPumpPoll::Event(
                                MarketMqttPumpEvent::SubscriptionAcknowledged,
                            ),
                            _ => MarketMqttPumpPoll::Event(MarketMqttPumpEvent::Fatal(
                                "market MQTT subscription was not granted at exact QoS 1"
                                    .to_owned(),
                            )),
                        }
                    }
                    Event::Incoming(Incoming::Publish(publish)) => {
                        match validate_publish(publish) {
                            Ok(message) => {
                                MarketMqttPumpPoll::Event(MarketMqttPumpEvent::Message(message))
                            }
                            Err(error) => MarketMqttPumpPoll::Event(MarketMqttPumpEvent::Fatal(
                                format!("market MQTT publish failed validation: {error:#}"),
                            )),
                        }
                    }
                    _ => MarketMqttPumpPoll::Idle,
                }
            })?;
        Ok(Self {
            _client: client,
            pump_receiver,
            _pump: pump,
            subscription_acks: 0,
            pending: VecDeque::new(),
        })
    }

    pub fn await_ready(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("market MQTT readiness deadline overflow")?;
        while self.subscription_acks < 2 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!("market MQTT subscriptions were not acknowledged in time")
            }
            let event = self
                .next_pump_event(remaining)?
                .context("market MQTT subscriptions were not acknowledged in time")?;
            match event {
                MarketMqttPumpEvent::SubscriptionAcknowledged => self.subscription_acks += 1,
                MarketMqttPumpEvent::Message(message) => self.pending.push_back(message),
                MarketMqttPumpEvent::Fatal(error) => bail!(error),
            }
        }
        Ok(())
    }

    pub fn receive(&mut self, timeout: Duration) -> Result<Option<MarketMqttMessage>> {
        if let Some(message) = self.pending.pop_front() {
            return Ok(Some(message));
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .context("market MQTT receive deadline overflow")?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let Some(event) = self.next_pump_event(remaining)? else {
                return Ok(None);
            };
            match event {
                MarketMqttPumpEvent::SubscriptionAcknowledged => self.subscription_acks += 1,
                MarketMqttPumpEvent::Message(message) => return Ok(Some(message)),
                MarketMqttPumpEvent::Fatal(error) => bail!(error),
            }
        }
    }

    fn next_pump_event(&self, timeout: Duration) -> Result<Option<MarketMqttPumpEvent>> {
        match self.pump_receiver.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
            Err(ChannelRecvTimeoutError::Timeout) => Ok(None),
            Err(ChannelRecvTimeoutError::Disconnected) => {
                bail!("market MQTT event pump disconnected")
            }
        }
    }
}

fn spawn_market_mqtt_pump<F>(
    name: String,
    mut poll: F,
) -> Result<(Receiver<MarketMqttPumpEvent>, JoinHandle<()>)>
where
    F: FnMut() -> MarketMqttPumpPoll + Send + 'static,
{
    let (pump_sender, pump_receiver) = mpsc::channel();
    let pump = thread::Builder::new()
        .name(name)
        .spawn(move || loop {
            let event = match poll() {
                MarketMqttPumpPoll::Idle => continue,
                MarketMqttPumpPoll::Event(event) => event,
            };
            let fatal = matches!(event, MarketMqttPumpEvent::Fatal(_));
            if pump_sender.send(event).is_err() || fatal {
                break;
            }
        })
        .context("failed to start the market MQTT event pump")?;
    Ok((pump_receiver, pump))
}

fn validate_publish(publish: rumqttc::v5::mqttbytes::v5::Publish) -> Result<MarketMqttMessage> {
    let topic = std::str::from_utf8(&publish.topic).context("market MQTT topic is not UTF-8")?;
    let content_type = publish
        .properties
        .as_ref()
        .and_then(|properties| properties.content_type.as_deref());
    MarketMqttMessage::from_committed_wire_contract(
        topic,
        &publish.payload,
        publish.qos,
        publish.retain,
        content_type,
    )
}

#[cfg(unix)]
fn validate_private_file(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to inspect {}", path.display()))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        bail!("market MQTT password file must be a private regular file")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pump_buffers_ordered_messages_and_fatal_while_main_consumer_is_blocked() -> Result<()> {
        let mut events = VecDeque::from([
            MarketMqttPumpEvent::Message(MarketMqttMessage {
                topic: "one".to_owned(),
                payload: vec![1],
            }),
            MarketMqttPumpEvent::Message(MarketMqttMessage {
                topic: "two".to_owned(),
                payload: vec![2],
            }),
            MarketMqttPumpEvent::Fatal("synthetic disconnect".to_owned()),
        ]);
        let (receiver, pump) = spawn_market_mqtt_pump("market-mqtt-test".to_owned(), move || {
            events
                .pop_front()
                .map(MarketMqttPumpPoll::Event)
                .unwrap_or(MarketMqttPumpPoll::Idle)
        })?;

        std::thread::sleep(Duration::from_millis(50));
        for expected in ["one", "two"] {
            match receiver.recv_timeout(Duration::from_secs(1))? {
                MarketMqttPumpEvent::Message(message) => assert_eq!(message.topic, expected),
                _ => panic!("ordered market message disappeared"),
            }
        }
        match receiver.recv_timeout(Duration::from_secs(1))? {
            MarketMqttPumpEvent::Fatal(error) => assert_eq!(error, "synthetic disconnect"),
            _ => panic!("terminal pump error disappeared"),
        }
        pump.join().expect("market MQTT test pump panicked");
        Ok(())
    }
}

#[cfg(not(unix))]
fn validate_private_file(path: &Path) -> Result<()> {
    if !fs::metadata(path)?.is_file() {
        bail!("market MQTT password file must be a regular file")
    }
    Ok(())
}
