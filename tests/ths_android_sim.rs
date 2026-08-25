use anyhow::{bail, Context, Result};
use gridedge_t::{
    domain::Direction,
    ths_android_sim::{
        cancellable_contract_ids_from_xml, classify_android_order_time, parse_android_fills_xml,
        parse_android_orders_xml, parse_top_resumed_package,
        validate_android_order_time_not_future, verify_cancel_confirmation_xml,
        verify_simulation_xml, verify_submit_confirmation_xml, verify_submit_outcome_xml,
        AdbExecutor, AndroidOrderTimeDisposition, AndroidThsConfig, AndroidThsSimulationUiDriver,
    },
    ths_sim::SimulatedOrderDraft,
    ths_sim_execution::SimulationUiDriver,
};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::{cell::RefCell, collections::VecDeque, rc::Rc, str::FromStr};

const PACKAGE: &str = "com.hexin.plat.android.supremacy";

fn node(text: &str, resource_id: &str, content_desc: &str, bounds: &str) -> String {
    format!(
        r#"<node text="{text}" resource-id="{resource_id}" class="android.widget.TextView" package="{PACKAGE}" content-desc="{content_desc}" clickable="false" bounds="{bounds}"/>"#
    )
}

fn form_xml(title: &str, account: &str, extra: &str) -> String {
    let controls = ["买入", "卖出", "撤单", "持仓", "查询"]
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            format!(
                r#"<node text="{label}" resource-id="{PACKAGE}:id/btn" class="android.widget.TextView" package="{PACKAGE}" content-desc="{label}" clickable="true" bounds="[{},268][{},385]"/>"#,
                index * 200,
                index * 200 + 180
            )
        })
        .collect::<String>();
    let fields = format!(
        r#"<node text="" resource-id="{PACKAGE}:id/auto_stockcode" class="android.widget.EditText" package="{PACKAGE}" content-desc="" clickable="true" bounds="[47,415][712,520]"/><node text="" resource-id="{PACKAGE}:id/stockprice" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[152,546][607,651]"><node text="" resource-id="" class="android.widget.EditText" package="{PACKAGE}" content-desc="" clickable="true" bounds="[152,546][607,651]"/></node><node text="" resource-id="{PACKAGE}:id/stockvolume" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[152,719][607,824]"><node text="" resource-id="" class="android.widget.EditText" package="{PACKAGE}" content-desc="" clickable="true" bounds="[152,719][607,824]"/></node>{}{}"#,
        node(
            "委托",
            &format!("{PACKAGE}:id/weituo"),
            "",
            "[270,1163][540,1219]"
        ),
        node(
            "成交",
            &format!("{PACKAGE}:id/chengjiao"),
            "",
            "[540,1163][810,1219]"
        )
    );
    let fields = format!(
        r#"<node text="" resource-id="{PACKAGE}:id/main_scroller" class="android.widget.ScrollView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,385][1080,2155]">{fields}</node>"#
    );
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}{}{}{fields}{extra}</node></hierarchy>"#,
        node(
            title,
            &format!("{PACKAGE}:id/page_title_view"),
            "",
            "[466,148][666,209]"
        ),
        node(
            account,
            &format!("{PACKAGE}:id/account_info_view"),
            "",
            "[511,212][621,255]"
        ),
        controls
    )
}

fn order_entry_form(code: &str, name: &str, price: &str, quantity: &str) -> String {
    let extra = format!(
        "{}<node text=\"买 入(模拟炒股)\" resource-id=\"{PACKAGE}:id/submit\" class=\"android.widget.Button\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"true\" bounds=\"[100,900][980,1000]\"/>",
        node(
            name,
            &format!("{PACKAGE}:id/stockname"),
            "",
            "[47,520][712,560]"
        )
    );
    let mut xml = form_xml("模拟炒股", "**0000", &extra);
    xml = xml.replacen(
        &format!("text=\"\" resource-id=\"{PACKAGE}:id/auto_stockcode\""),
        &format!("text=\"{code}\" resource-id=\"{PACKAGE}:id/auto_stockcode\""),
        1,
    );
    xml = xml.replacen(
        "text=\"\" resource-id=\"\" class=\"android.widget.EditText\" package=\"com.hexin.plat.android.supremacy\" content-desc=\"\" clickable=\"true\" bounds=\"[152,546][607,651]\"",
        &format!("text=\"{price}\" resource-id=\"\" class=\"android.widget.EditText\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"true\" bounds=\"[152,546][607,651]\""),
        1,
    );
    xml.replacen(
        "text=\"\" resource-id=\"\" class=\"android.widget.EditText\" package=\"com.hexin.plat.android.supremacy\" content-desc=\"\" clickable=\"true\" bounds=\"[152,719][607,824]\"",
        &format!("text=\"{quantity}\" resource-id=\"\" class=\"android.widget.EditText\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"true\" bounds=\"[152,719][607,824]\""),
        1,
    )
}

fn order_row(time: &str, status: &str, top: i32) -> String {
    order_row_with_filled(time, status, top, 0)
}

fn order_row_with_filled(time: &str, status: &str, top: i32, filled: i64) -> String {
    format!(
        r#"<node text="" resource-id="" class="android.widget.LinearLayout" package="{PACKAGE}" content-desc="" clickable="true" bounds="[0,{top}][1080,{}]"><node text="" resource-id="{PACKAGE}:id/container" class="android.widget.LinearLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,{top}][1080,{}]">{}{}{}{}{}{}{}{}</node></node>"#,
        top + 160,
        top + 160,
        node(
            "兆新股份",
            &format!("{PACKAGE}:id/result0"),
            "",
            "[47,397][267,453]"
        ),
        node(
            time,
            &format!("{PACKAGE}:id/result1"),
            "",
            "[95,453][267,509]"
        ),
        node(
            "3.370",
            &format!("{PACKAGE}:id/result2"),
            "",
            "[302,397][522,453]"
        ),
        node(
            "0.000",
            &format!("{PACKAGE}:id/result3"),
            "",
            "[302,453][522,509]"
        ),
        node(
            "700",
            &format!("{PACKAGE}:id/first_tv"),
            "",
            "[557,397][777,452]"
        ),
        node(
            &filled.to_string(),
            &format!("{PACKAGE}:id/second_tv"),
            "",
            "[557,452][777,509]"
        ),
        node(
            "买入",
            &format!("{PACKAGE}:id/result6"),
            "",
            "[812,397][1033,453]"
        ),
        node(
            status,
            &format!("{PACKAGE}:id/result7"),
            "",
            "[812,453][1033,509]"
        )
    )
}

fn fill_row(timestamp: &str, quantity: i64, top: i32) -> String {
    let amount = Decimal::from_str("3.360").unwrap() * Decimal::from(quantity);
    format!(
        r#"<node text="" resource-id="" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="true" bounds="[0,{top}][1080,{}]">{}{}{}{}{}{}</node>"#,
        top + 160,
        node("兆新股份", "", "", "[46,643][271,710]"),
        node(timestamp, "", "", "[94,710][271,760]"),
        node("3.360", "", "", "[294,622][556,782]"),
        node(&quantity.to_string(), "", "", "[556,622][818,782]"),
        node("买入", "", "", "[818,643][1034,710]"),
        node(
            &amount.normalize().to_string(),
            "",
            "",
            "[818,710][1034,760]"
        )
    )
}

fn fill_xml(rows: &str) -> String {
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}{}{}{}<node text="" resource-id="{PACKAGE}:id/recyclerview_id" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,622][1080,2155]">{rows}</node></node></hierarchy>"#,
        node("成交时间", "", "", "[0,516][250,621]"),
        node("成交价", "", "", "[250,516][500,621]"),
        node("成交量", "", "", "[500,516][750,621]"),
        node("成交额", "", "", "[750,516][1000,621]")
    )
}

fn order_xml(rows: &str) -> String {
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}{}{}{}<node text="" resource-id="{PACKAGE}:id/container" class="android.widget.LinearLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,350][1080,1800]">{rows}</node></node></hierarchy>"#,
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]")
    )
}

fn detail_body() -> String {
    format!(
        r#"{}{}{}{}{}{}"#,
        node(
            "委托撤单确认",
            &format!("{PACKAGE}:id/title_view"),
            "",
            "[300,300][780,360]"
        ),
        node(
            "操作  撤买入单",
            &format!("{PACKAGE}:id/option_textview"),
            "",
            "[100,400][900,450]"
        ),
        node(
            "名称  兆新股份",
            &format!("{PACKAGE}:id/stockname_textview"),
            "",
            "[100,460][900,510]"
        ),
        node(
            "代码  002256",
            &format!("{PACKAGE}:id/stockcode_textview"),
            "",
            "[100,520][900,570]"
        ),
        node(
            "数量  700",
            &format!("{PACKAGE}:id/ordernumber_textview"),
            "",
            "[100,580][900,630]"
        ),
        node(
            "价格  3.370",
            &format!("{PACKAGE}:id/orderprice_textview"),
            "",
            "[100,640][900,690]"
        )
    )
}

fn detail_xml() -> String {
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}</node></hierarchy>"#,
        detail_body()
    )
}

fn final_cancel_xml() -> String {
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}<node text="您是否确认以上撤单？" resource-id="{PACKAGE}:id/tips_textview" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[100,690][900,730]"/><node text="撤单" resource-id="{PACKAGE}:id/option_chedan" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="true" bounds="[100,730][900,780]"/><node text="撤单后继续买入" resource-id="{PACKAGE}:id/option_chedan_and_buy" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="true" bounds="[100,780][900,830]"/><node text="取消" resource-id="{PACKAGE}:id/option_cancel" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="true" bounds="[100,830][900,880]"/></node></hierarchy>"#,
        detail_body()
    )
}

fn cancellable_region(rows: &str) -> String {
    format!(
        "{}{}{}{}<node text=\"\" resource-id=\"{PACKAGE}:id/chedan_recycler_view\" class=\"androidx.recyclerview.widget.RecyclerView\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"false\" bounds=\"[0,492][1080,1306]\">{rows}</node>",
        node("委托时间", "", "", "[0,386][220,491]"),
        node("委托/均价", "", "", "[220,386][440,491]"),
        node("委托/成交", "", "", "[440,386][660,491]"),
        node("状态", "", "", "[660,386][880,491]")
    )
}

fn submit_confirmation_xml(account: &str, symbol: &str, quantity: &str, price: &str) -> String {
    let field = |text: &str, id: &str, top: i32| {
        node(
            text,
            &format!("{PACKAGE}:id/{id}"),
            "",
            &format!("[200,{top}][880,{}]", top + 50),
        )
    };
    let price_and_tip = format!(
        "{}{}",
        field(price, "price_value", 1234),
        field("您是否确认以上委托?", "confirm_tips", 1339)
    );
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[132,838][948,1594]">{}{}{}{}{}{}<node text="取消" resource-id="{PACKAGE}:id/cancel_btn" class="android.widget.Button" package="{PACKAGE}" content-desc="" clickable="true" bounds="[132,1466][540,1594]"/><node text="确认买入" resource-id="{PACKAGE}:id/ok_btn" class="android.widget.Button" package="{PACKAGE}" content-desc="" clickable="true" bounds="[541,1466][948,1594]"/></node></hierarchy>"#,
        field("委托买入确认", "dialog_title", 908),
        field(account, "account_value", 1021),
        field("兆新股份", "stock_name_value", 1126),
        field(symbol, "stock_code_value", 1126),
        field(quantity, "number_value", 1234),
        price_and_tip
    )
}

fn submit_outcome_xml(prompt: &str) -> String {
    format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[132,1024][948,1478]">{}{}<node text="确定" resource-id="{PACKAGE}:id/ok_btn" class="android.widget.Button" package="{PACKAGE}" content-desc="" clickable="true" bounds="[132,1350][948,1478]"/></node></hierarchy>"#,
        node(
            "系统信息",
            &format!("{PACKAGE}:id/dialog_title"),
            "",
            "[132,1024][948,1127]"
        ),
        node(
            prompt,
            &format!("{PACKAGE}:id/prompt_content"),
            "",
            "[132,1127][948,1349]"
        )
    )
}

#[test]
fn reviewed_android_simulation_identity_requires_exact_title_account_and_controls() -> Result<()> {
    let config = AndroidThsConfig::default();
    let evidence = verify_simulation_xml(&form_xml("模拟炒股", "**0000", ""), &config)?;
    assert_eq!(evidence.masked_account, "**0000");
    assert_eq!(
        evidence.control_labels,
        ["买入", "卖出", "撤单", "持仓", "查询"]
    );

    assert!(verify_simulation_xml(&form_xml("中信证券", "**0000", ""), &config).is_err());
    assert!(verify_simulation_xml(&form_xml("模拟炒股", "**9999", ""), &config).is_err());
    assert!(verify_simulation_xml(
        &form_xml(
            "模拟炒股",
            "**0000",
            &node("中信证券", "", "", "[10,400][200,450]")
        ),
        &config
    )
    .is_err());
    Ok(())
}

#[test]
fn resumed_component_package_is_exact_and_unique() -> Result<()> {
    assert_eq!(
        parse_top_resumed_package(&format!(
            "topResumedActivity=ActivityRecord{{x u0 {PACKAGE}/com.hexin.plat.android.Hexin}}\n"
        ))?,
        PACKAGE
    );
    assert_ne!(
        parse_top_resumed_package(&format!(
            "topResumedActivity=ActivityRecord{{x u0 {PACKAGE}.evil/.Hexin}}\n"
        ))?,
        PACKAGE
    );
    assert!(parse_top_resumed_package(&format!(
        "topResumedActivity=ActivityRecord{{x u0 {PACKAGE}/.A}}\ntopResumedActivity=ActivityRecord{{y u0 {PACKAGE}/.B}}\n"
    ))
    .is_err());
    Ok(())
}

#[test]
fn current_day_order_time_cannot_be_after_the_observed_device_time() -> Result<()> {
    let observed =
        chrono::NaiveDateTime::parse_from_str("2026-08-25 08:01:00", "%Y-%m-%d %H:%M:%S")?;
    validate_android_order_time_not_future("08:01:00", observed)?;
    assert!(validate_android_order_time_not_future("18:27:29", observed).is_err());
    assert!(validate_android_order_time_not_future("25:00:00", observed).is_err());
    assert_eq!(
        classify_android_order_time("18:27:29", "全部撤单", observed)?,
        AndroidOrderTimeDisposition::QuarantinedTerminalPreviousSession
    );
    assert!(classify_android_order_time("18:27:29", "已报", observed).is_err());
    let open = chrono::NaiveDateTime::parse_from_str("2026-08-25 09:30:00", "%Y-%m-%d %H:%M:%S")?;
    assert_eq!(
        classify_android_order_time("18:27:29", "全部撤单", open)?,
        AndroidOrderTimeDisposition::QuarantinedTerminalPreviousSession
    );
    assert!(classify_android_order_time("18:27:29", "已报", open).is_err());
    Ok(())
}

#[test]
fn android_order_contract_is_stable_and_duplicate_rows_fail_closed() -> Result<()> {
    let config = AndroidThsConfig::default();
    let row = order_row("11:00:00", "未成交", 364);
    let first =
        parse_android_orders_xml(&order_xml(&row), &[&detail_xml()], "2026-08-25", &config)?;
    let second =
        parse_android_orders_xml(&order_xml(&row), &[&detail_xml()], "2026-08-25", &config)?;
    let first_records = first.records()?;
    let second_records = second.records()?;
    assert_eq!(first_records[0].contract_id, second_records[0].contract_id);
    assert!(first_records[0].contract_id.starts_with("ANDROID-"));
    assert_eq!(first_records[0].symbol, "002256");

    let duplicate = format!("{}{}", row, order_row("11:00:00", "未成交", 524));
    assert!(parse_android_orders_xml(
        &order_xml(&duplicate),
        &[&detail_xml(), &detail_xml()],
        "2026-08-25",
        &config
    )
    .is_err());
    Ok(())
}

#[test]
fn cancel_candidates_come_only_from_cancel_tab_cancellable_region() -> Result<()> {
    let config = AndroidThsConfig::default();
    let headers = format!(
        "{}{}{}{}",
        node("委托时间", "", "", "[0,386][220,491]"),
        node("委托/均价", "", "", "[220,386][440,491]"),
        node("委托/成交", "", "", "[440,386][660,491]"),
        node("状态", "", "", "[660,386][880,491]")
    );
    let empty = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{headers}<node text="" resource-id="{PACKAGE}:id/chedan_recycler_view" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1306]"><node text="" resource-id="{PACKAGE}:id/chedan_empty_layout" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1040]">{}</node><node text="" resource-id="" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="true" bounds="[0,1040][1080,1145]">{}</node>{}</node></node></hierarchy>"#,
        node(
            "当前没有可撤委托单",
            &format!("{PACKAGE}:id/nodata_tips"),
            "",
            "[0,898][1080,957]"
        ),
        node(
            "其他",
            &format!("{PACKAGE}:id/cannot_chedan_title_text"),
            "",
            "[501,1066][579,1119]"
        ),
        order_row("11:00:00", "全部撤单", 1145)
    );
    assert!(cancellable_contract_ids_from_xml(&empty, "2026-08-25", &config)?.is_empty());

    let cancellable = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{headers}<node text="" resource-id="{PACKAGE}:id/chedan_recycler_view" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1306]">{}<node text="" resource-id="" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="true" bounds="[0,700][1080,800]">{}</node>{}</node></node></hierarchy>"#,
        order_row("11:00:00", "已报", 500),
        node(
            "其他",
            &format!("{PACKAGE}:id/cannot_chedan_title_text"),
            "",
            "[501,720][579,770]"
        ),
        order_row("10:00:00", "全部撤单", 820)
    );
    let ids = cancellable_contract_ids_from_xml(&cancellable, "2026-08-25", &config)?;
    assert_eq!(ids.len(), 1);
    Ok(())
}

#[test]
fn cancel_candidates_require_structural_membership_and_unique_identity() -> Result<()> {
    let config = AndroidThsConfig::default();
    let headers = format!(
        "{}{}{}{}",
        node("委托时间", "", "", "[0,386][220,491]"),
        node("委托/均价", "", "", "[220,386][440,491]"),
        node("委托/成交", "", "", "[440,386][660,491]"),
        node("状态", "", "", "[660,386][880,491]")
    );
    let external_collision = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{headers}{}<node text="" resource-id="{PACKAGE}:id/chedan_recycler_view" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1306]">{}{}</node></node></hierarchy>"#,
        order_row("11:00:00", "已报", 500),
        node(
            "其他",
            &format!("{PACKAGE}:id/cannot_chedan_title_text"),
            "",
            "[501,720][579,770]"
        ),
        order_row("11:00:00", "全部撤单", 500)
    );
    assert!(
        cancellable_contract_ids_from_xml(&external_collision, "2026-08-25", &config)?.is_empty()
    );

    let duplicate = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{headers}<node text="" resource-id="{PACKAGE}:id/chedan_recycler_view" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1306]">{}{}</node></node></hierarchy>"#,
        order_row("11:00:00", "已报", 500),
        order_row("11:00:00", "已报", 700)
    );
    assert!(cancellable_contract_ids_from_xml(&duplicate, "2026-08-25", &config).is_err());
    Ok(())
}

#[test]
fn foreign_package_detail_and_invalid_device_calendar_fail_closed() {
    let config = AndroidThsConfig::default();
    let row = order_row("11:00:00", "未成交", 364);
    let foreign_detail = detail_xml().replace(PACKAGE, "com.hexin.plat.android.supremacy.evil");
    assert!(parse_android_orders_xml(
        &order_xml(&row),
        &[foreign_detail.as_str()],
        "2026-08-25",
        &config
    )
    .is_err());
    assert!(
        parse_android_orders_xml(&order_xml(&row), &[&detail_xml()], "2026-02-30", &config)
            .is_err()
    );
}

#[test]
fn final_cancel_confirmation_rechecks_simulation_identity_and_exact_package() -> Result<()> {
    let config = AndroidThsConfig::default();
    let action = format!(
        r#"<node text="您是否确认以上撤单？" resource-id="{PACKAGE}:id/tips_textview" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[100,690][900,730]"/><node text="撤单" resource-id="{PACKAGE}:id/option_chedan" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="true" bounds="[100,730][900,780]"/><node text="撤单后继续买入" resource-id="{PACKAGE}:id/option_chedan_and_buy" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="true" bounds="[100,780][900,830]"/><node text="取消" resource-id="{PACKAGE}:id/option_cancel" class="android.widget.TextView" package="{PACKAGE}" content-desc="" clickable="true" bounds="[100,830][900,880]"/>"#
    );
    let extra = format!("{}{}", detail_body(), action);
    verify_cancel_confirmation_xml(&form_xml("模拟炒股", "**0000", &extra), &config)?;
    assert!(
        verify_cancel_confirmation_xml(&form_xml("中信证券", "**0000", &extra), &config).is_err()
    );
    assert!(
        verify_cancel_confirmation_xml(&form_xml("模拟炒股", "**9999", &extra), &config).is_err()
    );
    let foreign = form_xml("模拟炒股", "**0000", &extra)
        .replace(PACKAGE, "com.hexin.plat.android.supremacy.evil");
    assert!(verify_cancel_confirmation_xml(&foreign, &config).is_err());
    Ok(())
}

#[test]
fn submit_confirmation_binds_simulation_account_and_exact_order() -> Result<()> {
    let account = "SIM-ACCOUNT-1";
    let config = AndroidThsConfig {
        confirmation_account_sha256: Some(hex::encode(Sha256::digest(account.as_bytes()))),
        money_actions_enabled: true,
        ..AndroidThsConfig::default()
    };
    let order = SimulatedOrderDraft {
        direction: Direction::Buy,
        symbol: "002256".to_owned(),
        limit_price: Decimal::from_str("3.00")?,
        quantity: 100,
    };
    verify_submit_confirmation_xml(
        &submit_confirmation_xml(account, "002256", "100", "3"),
        &config,
        &order,
    )?;
    assert!(verify_submit_confirmation_xml(
        &submit_confirmation_xml("OTHER", "002256", "100", "3"),
        &config,
        &order,
    )
    .is_err());
    assert!(verify_submit_confirmation_xml(
        &submit_confirmation_xml(account, "002256", "200", "3"),
        &config,
        &order,
    )
    .is_err());
    assert!(verify_submit_confirmation_xml(
        &submit_confirmation_xml(account, "002257", "100", "3"),
        &config,
        &order,
    )
    .is_err());
    Ok(())
}

#[test]
fn submit_outcome_requires_one_numeric_simulation_contract() -> Result<()> {
    let config = AndroidThsConfig::default();
    let outcome = |prompt: &str| {
        format!(
            r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[132,1024][948,1478]">{}{}<node text="确定" resource-id="{PACKAGE}:id/ok_btn" class="android.widget.Button" package="{PACKAGE}" content-desc="" clickable="true" bounds="[132,1350][948,1478]"/></node></hierarchy>"#,
            node(
                "系统信息",
                &format!("{PACKAGE}:id/dialog_title"),
                "",
                "[132,1024][948,1127]"
            ),
            node(
                prompt,
                &format!("{PACKAGE}:id/prompt_content"),
                "",
                "[132,1127][948,1349]"
            )
        )
    };
    verify_submit_outcome_xml(&outcome("委托已提交，合同号为：6219693701"), &config)?;
    assert!(verify_submit_outcome_xml(&outcome("委托失败"), &config).is_err());
    assert!(verify_submit_outcome_xml(&outcome("委托已提交，合同号为：ABC"), &config).is_err());
    Ok(())
}

#[test]
fn fill_is_assigned_only_to_one_compatible_android_order() -> Result<()> {
    let config = AndroidThsConfig::default();
    let orders = parse_android_orders_xml(
        &order_xml(&order_row_with_filled("11:00:00", "全部成交", 364, 700)),
        &[&detail_xml()],
        "2026-08-25",
        &config,
    )?;
    let fill_xml = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}{}{}{}<node text="" resource-id="{PACKAGE}:id/recyclerview_id" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,622][1080,2155]"><node text="" resource-id="" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="true" bounds="[0,622][1080,782]">{}{}{}{}{}{}</node></node></node></hierarchy>"#,
        node("成交时间", "", "", "[0,516][250,621]"),
        node("成交价", "", "", "[250,516][500,621]"),
        node("成交量", "", "", "[500,516][750,621]"),
        node("成交额", "", "", "[750,516][1000,621]"),
        node("兆新股份", "", "", "[46,643][271,710]"),
        node("20260825 11:02:12", "", "", "[94,710][271,760]"),
        node("3.360", "", "", "[294,622][556,782]"),
        node("700", "", "", "[556,622][818,782]"),
        node("买入", "", "", "[818,643][1034,710]"),
        node("2352.000", "", "", "[818,710][1034,760]")
    );
    let fills = parse_android_fills_xml(&fill_xml, &orders, &config)?;
    let records = fills.records()?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].contract_id, orders.records()?[0].contract_id);
    assert_eq!(records[0].amount, Decimal::from_str("2352.000")?);
    Ok(())
}

#[test]
fn current_day_fill_time_uses_the_unique_order_date() -> Result<()> {
    let config = AndroidThsConfig::default();
    let orders = parse_android_orders_xml(
        &order_xml(&order_row_with_filled("11:00:00", "全部成交", 364, 700)),
        &[&detail_xml()],
        "2026-08-25",
        &config,
    )?;
    let fill_xml = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}{}{}{}<node text="" resource-id="{PACKAGE}:id/recyclerview_id" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,622][1080,2155]"><node text="" resource-id="" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="true" bounds="[0,622][1080,782]">{}{}{}{}{}{}</node></node></node></hierarchy>"#,
        node("成交时间", "", "", "[0,516][250,621]"),
        node("成交价", "", "", "[250,516][500,621]"),
        node("成交量", "", "", "[500,516][750,621]"),
        node("成交额", "", "", "[750,516][1000,621]"),
        node("兆新股份", "", "", "[46,643][271,710]"),
        node("11:02:12", "", "", "[94,710][271,760]"),
        node("3.360", "", "", "[294,622][556,782]"),
        node("700", "", "", "[556,622][818,782]"),
        node("买入", "", "", "[818,643][1034,710]"),
        node("2352.000", "", "", "[818,710][1034,760]")
    );
    let records = parse_android_fills_xml(&fill_xml, &orders, &config)?.records()?;
    assert_eq!(records[0].fill_date, "2026-08-25");
    assert_eq!(records[0].fill_time, "11:02:12");
    Ok(())
}

#[test]
fn fills_fail_for_zero_or_multiple_order_candidates_and_cumulative_overfill() -> Result<()> {
    let config = AndroidThsConfig::default();
    let one_order = parse_android_orders_xml(
        &order_xml(&order_row_with_filled("11:00:00", "全部成交", 364, 700)),
        &[&detail_xml()],
        "2026-08-25",
        &config,
    )?;
    assert!(parse_android_fills_xml(
        &fill_xml(&fill_row("10:59:59", 100, 622)),
        &one_order,
        &config
    )
    .is_err());

    let two_rows = format!(
        "{}{}",
        order_row_with_filled("10:00:00", "全部成交", 364, 700),
        order_row_with_filled("10:30:00", "全部成交", 524, 700)
    );
    let two_orders = parse_android_orders_xml(
        &order_xml(&two_rows),
        &[&detail_xml(), &detail_xml()],
        "2026-08-25",
        &config,
    )?;
    assert!(parse_android_fills_xml(
        &fill_xml(&fill_row("11:02:12", 100, 622)),
        &two_orders,
        &config
    )
    .is_err());

    let overfill_rows = format!(
        "{}{}",
        fill_row("11:02:12", 400, 622),
        fill_row("11:03:12", 400, 782)
    );
    assert!(parse_android_fills_xml(&fill_xml(&overfill_rows), &one_order, &config).is_err());
    Ok(())
}

#[test]
fn embedded_current_fill_empty_marker_is_a_valid_empty_table() -> Result<()> {
    let config = AndroidThsConfig::default();
    let xml = format!(
        r#"<?xml version="1.0"?><hierarchy><node text="" resource-id="" class="android.widget.FrameLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,0][1080,2340]">{}</node></hierarchy>"#,
        node(
            "没有成交数据",
            &format!("{PACKAGE}:id/tv_add_stock"),
            "",
            "[408,1474][672,1533]"
        )
    );
    let orders = parse_android_orders_xml(&order_xml(""), &[], "2026-08-25", &config)?;
    assert!(parse_android_fills_xml(&xml, &orders, &config)?
        .rows
        .is_empty());
    Ok(())
}

#[derive(Default)]
struct NeverAdb;

impl AdbExecutor for NeverAdb {
    fn run(&mut self, _args: &[String]) -> Result<String> {
        bail!("ADB must not be called while money actions are disabled")
    }
}

#[test]
fn android_money_actions_are_disabled_by_default_before_any_adb_call() {
    let config = AndroidThsConfig::default();
    let mut driver = AndroidThsSimulationUiDriver::with_executor(config, NeverAdb);
    let order = SimulatedOrderDraft {
        direction: Direction::Buy,
        symbol: "002256".to_owned(),
        limit_price: Decimal::from_str("3.30").unwrap(),
        quantity: 100,
    };
    assert!(driver.submit_once(&order).is_err());
    assert!(driver.cancel_contract_once("ANDROID-deadbeef").is_err());
}

struct IdentityAdb {
    devices: String,
    avd: String,
    version: String,
    activities: String,
    dump_failures_remaining: usize,
}

impl AdbExecutor for IdentityAdb {
    fn run(&mut self, args: &[String]) -> Result<String> {
        if args == ["devices", "-l"] {
            return Ok(self.devices.clone());
        }
        let joined = args.join(" ");
        if joined.contains("getprop ro.boot.qemu.avd_name") {
            return Ok(format!("{}\n", self.avd));
        }
        if joined.contains("dumpsys package") {
            return Ok(format!("versionName={}\n", self.version));
        }
        if joined.contains("dumpsys activity activities") {
            return Ok(self.activities.clone());
        }
        if joined.contains("uiautomator dump") && self.dump_failures_remaining > 0 {
            self.dump_failures_remaining -= 1;
            bail!("transient uiautomator dump failure");
        }
        if joined.contains("exec-out cat") {
            return Ok(form_xml("模拟炒股", "**0000", ""));
        }
        Ok(String::new())
    }
}

fn identity_adb() -> IdentityAdb {
    IdentityAdb {
        devices: "List of devices attached\nemulator-5554 device product:test\n".to_owned(),
        avd: "THSP_API_32".to_owned(),
        version: "10.94.09".to_owned(),
        activities: format!(
            "topResumedActivity=ActivityRecord{{x u0 {PACKAGE}/com.hexin.plat.android.Hexin}}\n"
        ),
        dump_failures_remaining: 0,
    }
}

#[test]
fn device_identity_tolerates_five_transient_ui_dump_failures_without_weakening_identity() {
    let config = AndroidThsConfig::default();
    let mut transient = identity_adb();
    transient.dump_failures_remaining = 5;
    let mut driver = AndroidThsSimulationUiDriver::with_executor(config, transient);

    let evidence = driver
        .probe_identity()
        .expect("sixth exact snapshot succeeds");
    assert_eq!(evidence.avd_name, "THSP_API_32");
    assert_eq!(evidence.package, PACKAGE);
    assert_eq!(evidence.masked_account, "**0000");
}

#[test]
fn device_identity_rejects_wrong_serial_avd_version_package_and_multiple_resumed() {
    let config = AndroidThsConfig::default();
    let mut wrong_serial = identity_adb();
    wrong_serial.devices =
        "List of devices attached\nemulator-5556 device product:test\n".to_owned();
    assert!(
        AndroidThsSimulationUiDriver::with_executor(config.clone(), wrong_serial)
            .probe_identity()
            .is_err()
    );

    let mut wrong_avd = identity_adb();
    wrong_avd.avd = "Pixel_5_API_31".to_owned();
    assert!(
        AndroidThsSimulationUiDriver::with_executor(config.clone(), wrong_avd)
            .probe_identity()
            .is_err()
    );

    let mut wrong_version = identity_adb();
    wrong_version.version = "10.94.10".to_owned();
    assert!(
        AndroidThsSimulationUiDriver::with_executor(config.clone(), wrong_version)
            .probe_identity()
            .is_err()
    );

    let mut substring_package = identity_adb();
    substring_package.activities =
        format!("topResumedActivity=ActivityRecord{{x u0 {PACKAGE}.evil/.Hexin}}\n");
    assert!(
        AndroidThsSimulationUiDriver::with_executor(config.clone(), substring_package)
            .probe_identity()
            .is_err()
    );

    let mut multiple = identity_adb();
    multiple.activities.push_str(&format!(
        "topResumedActivity=ActivityRecord{{y u0 {PACKAGE}/.Other}}\n"
    ));
    assert!(
        AndroidThsSimulationUiDriver::with_executor(config, multiple)
            .probe_identity()
            .is_err()
    );
}

#[derive(Clone)]
struct ScriptedAdb {
    snapshots: Rc<RefCell<VecDeque<String>>>,
    commands: Rc<RefCell<Vec<Vec<String>>>>,
    device_time: Option<String>,
}

type ScriptedMoneyDriver = (
    AndroidThsSimulationUiDriver<ScriptedAdb>,
    Rc<RefCell<Vec<Vec<String>>>>,
);

impl AdbExecutor for ScriptedAdb {
    fn run(&mut self, args: &[String]) -> Result<String> {
        self.commands.borrow_mut().push(args.to_vec());
        if args == ["devices", "-l"] {
            return Ok(
                "List of devices attached\nemulator-5554          device product:test\n".to_owned(),
            );
        }
        let joined = args.join(" ");
        if joined.contains("getprop ro.boot.qemu.avd_name") {
            return Ok("THSP_API_32\n".to_owned());
        }
        if joined.contains("dumpsys package") {
            return Ok("versionCode=4941\nversionName=10.94.09\n".to_owned());
        }
        if joined.contains("dumpsys activity activities") {
            return Ok(format!(
                "topResumedActivity=ActivityRecord{{x u0 {PACKAGE}/com.hexin.plat.android.Hexin}}\n"
            ));
        }
        if joined.contains("shell date +%Y-%m-%dT%H:%M:%S%z") {
            if let Some(device_time) = &self.device_time {
                return Ok(format!("{device_time}\n"));
            }
            return Ok(format!(
                "{}\n",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%z")
            ));
        }
        if joined.contains("exec-out cat") {
            return self
                .snapshots
                .borrow_mut()
                .pop_front()
                .context("scripted Android UI snapshot is missing");
        }
        Ok(String::new())
    }

    fn host_now(&self) -> chrono::DateTime<chrono::FixedOffset> {
        self.device_time
            .as_deref()
            .and_then(|value| chrono::DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%z").ok())
            .unwrap_or_else(|| chrono::Local::now().fixed_offset())
    }
}

fn scripted_money_driver(
    outcome_snapshots: impl IntoIterator<Item = String>,
) -> ScriptedMoneyDriver {
    let account = "SIM-ACCOUNT-1";
    let mut snapshots = VecDeque::from([
        order_entry_form("", "", "", ""),
        order_entry_form("", "", "", ""),
        order_entry_form("002256", "兆新股份", "", ""),
        order_entry_form("002256", "兆新股份", "3", ""),
        order_entry_form("002256", "兆新股份", "3", "100"),
        order_entry_form("002256", "兆新股份", "3", "100"),
        submit_confirmation_xml(account, "002256", "100", "3"),
    ]);
    snapshots.extend(outcome_snapshots);
    let commands = Rc::new(RefCell::new(Vec::new()));
    let executor = ScriptedAdb {
        snapshots: Rc::new(RefCell::new(snapshots)),
        commands: commands.clone(),
        device_time: None,
    };
    let config = AndroidThsConfig {
        confirmation_account_sha256: Some(hex::encode(Sha256::digest(account.as_bytes()))),
        money_actions_enabled: true,
        ..AndroidThsConfig::default()
    };
    (
        AndroidThsSimulationUiDriver::with_executor(config, executor),
        commands,
    )
}

fn buy_100_at_3() -> SimulatedOrderDraft {
    SimulatedOrderDraft {
        direction: Direction::Buy,
        symbol: "002256".to_owned(),
        limit_price: Decimal::from_str("3.00").unwrap(),
        quantity: 100,
    }
}

#[test]
fn money_enabled_driver_prepare_submit_confirmation_and_success_each_click_once() -> Result<()> {
    let (mut driver, commands) =
        scripted_money_driver([submit_outcome_xml("委托已提交，合同号为：6219693701")]);
    let order = buy_100_at_3();
    driver.prepare(&order)?;
    driver.submit_once(&order)?;

    let taps = commands
        .borrow()
        .iter()
        .map(|command| command.join(" "))
        .filter(|command| command.contains(" shell input tap "))
        .collect::<Vec<_>>();
    for final_target in ["tap 540 950", "tap 744 1530", "tap 540 1414"] {
        assert_eq!(
            taps.iter()
                .filter(|command| command.ends_with(final_target))
                .count(),
            1,
            "{final_target} must be clicked exactly once"
        );
    }
    Ok(())
}

#[test]
fn outcome_probe_failure_never_repeats_the_final_money_click() -> Result<()> {
    let invalid = submit_outcome_xml("委托处理中");
    let (mut driver, commands) = scripted_money_driver(std::iter::repeat_n(invalid, 5));
    let order = buy_100_at_3();
    driver.prepare(&order)?;
    assert!(driver.submit_once(&order).is_err());

    let confirmation_clicks = commands
        .borrow()
        .iter()
        .map(|command| command.join(" "))
        .filter(|command| command.ends_with("tap 744 1530"))
        .count();
    assert_eq!(confirmation_clicks, 1);
    Ok(())
}

#[test]
fn money_enabled_driver_cancels_only_from_top_tab_and_final_action_clicks_once() -> Result<()> {
    let config = AndroidThsConfig {
        confirmation_account_sha256: Some("a".repeat(64)),
        money_actions_enabled: true,
        ..AndroidThsConfig::default()
    };
    let date = chrono::Local::now().date_naive().to_string();
    let row = order_row("00:00:00", "未成交", 700);
    let contract = parse_android_orders_xml(&order_xml(&row), &[&detail_xml()], &date, &config)?
        .records()?[0]
        .contract_id
        .clone();
    let order_region = format!(
        "{}{}{}{}{}",
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]"),
        row
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &order_region),
        detail_xml(),
        form_xml("模拟炒股", "**0000", ""),
        form_xml(
            "模拟炒股",
            "**0000",
            &cancellable_region(&order_row("00:00:00", "未成交", 700)),
        ),
        final_cancel_xml(),
    ])));
    let commands = Rc::new(RefCell::new(Vec::new()));
    let executor = ScriptedAdb {
        snapshots,
        commands: commands.clone(),
        device_time: None,
    };
    let mut driver = AndroidThsSimulationUiDriver::with_executor(config, executor);
    driver.cancel_contract_once(&contract)?;

    let taps = commands
        .borrow()
        .iter()
        .map(|command| command.join(" "))
        .filter(|command| command.contains(" shell input tap "))
        .collect::<Vec<_>>();
    assert_eq!(
        taps.iter()
            .filter(|command| command.ends_with("tap 500 755"))
            .count(),
        1
    );
    assert!(taps.iter().any(|command| command.ends_with("tap 490 326")));
    Ok(())
}

#[test]
fn read_only_cancel_probe_taps_top_cancel_tab_not_query_or_embedded_orders() -> Result<()> {
    let headers = format!(
        "{}{}{}{}",
        node("委托时间", "", "", "[0,386][220,491]"),
        node("委托/均价", "", "", "[220,386][440,491]"),
        node("委托/成交", "", "", "[440,386][660,491]"),
        node("状态", "", "", "[660,386][880,491]")
    );
    let cancel_region = format!(
        r#"{headers}<node text="" resource-id="{PACKAGE}:id/chedan_recycler_view" class="androidx.recyclerview.widget.RecyclerView" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1306]"><node text="" resource-id="{PACKAGE}:id/chedan_empty_layout" class="android.widget.RelativeLayout" package="{PACKAGE}" content-desc="" clickable="false" bounds="[0,492][1080,1040]">{}</node></node>"#,
        node(
            "当前没有可撤委托单",
            &format!("{PACKAGE}:id/nodata_tips"),
            "",
            "[0,898][1080,957]"
        )
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &cancel_region),
    ])));
    let commands = Rc::new(RefCell::new(Vec::new()));
    let executor = ScriptedAdb {
        snapshots,
        commands: commands.clone(),
        device_time: None,
    };
    let mut driver =
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor);
    assert!(driver.probe_cancellable_contract_ids()?.is_empty());

    let taps = commands
        .borrow()
        .iter()
        .filter(|command| command.join(" ").contains("shell input tap"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(taps.len(), 1);
    assert_eq!(taps[0][4..], ["tap", "490", "326"]);
    Ok(())
}

#[test]
fn startup_preflight_runs_identity_orders_fills_and_top_cancel_on_one_driver() -> Result<()> {
    let order_region = format!(
        "{}{}{}{}",
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]")
    );
    let fill_region = format!(
        "{}{}{}{}<node text=\"\" resource-id=\"{PACKAGE}:id/recyclerview_id\" class=\"androidx.recyclerview.widget.RecyclerView\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"false\" bounds=\"[0,622][1080,2155]\"></node>",
        node("成交时间", "", "", "[0,516][250,621]"),
        node("成交价", "", "", "[250,516][500,621]"),
        node("成交量", "", "", "[500,516][750,621]"),
        node("成交额", "", "", "[750,516][1000,621]")
    );
    let cancel_region = format!(
        "{}{}{}{}<node text=\"\" resource-id=\"{PACKAGE}:id/chedan_recycler_view\" class=\"androidx.recyclerview.widget.RecyclerView\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"false\" bounds=\"[0,492][1080,1306]\"><node text=\"\" resource-id=\"{PACKAGE}:id/chedan_empty_layout\" class=\"android.widget.RelativeLayout\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"false\" bounds=\"[0,492][1080,1040]\">{}</node></node>",
        node("委托时间", "", "", "[0,386][220,491]"),
        node("委托/均价", "", "", "[220,386][440,491]"),
        node("委托/成交", "", "", "[440,386][660,491]"),
        node("状态", "", "", "[660,386][880,491]"),
        node(
            "当前没有可撤委托单",
            &format!("{PACKAGE}:id/nodata_tips"),
            "",
            "[0,898][1080,957]"
        )
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &order_region),
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &fill_region),
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &cancel_region),
    ])));
    let commands = Rc::new(RefCell::new(Vec::new()));
    let executor = ScriptedAdb {
        snapshots,
        commands: commands.clone(),
        device_time: None,
    };
    let mut driver =
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor);
    driver.startup_preflight()?;
    let taps = commands
        .borrow()
        .iter()
        .filter(|command| command.join(" ").contains("shell input tap"))
        .count();
    assert_eq!(taps, 3);
    Ok(())
}

#[test]
fn duplicate_terminal_preopen_rows_fail_closed_before_quarantine() -> Result<()> {
    let duplicate_rows = format!(
        "{}{}{}{}{}{}",
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]"),
        order_row("18:27:29", "全部撤单", 364),
        order_row("18:27:29", "全部撤单", 700)
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &duplicate_rows),
        detail_xml(),
        detail_xml(),
    ])));
    let executor = ScriptedAdb {
        snapshots,
        commands: Rc::new(RefCell::new(Vec::new())),
        device_time: Some("2026-08-25T08:01:00+0800".to_owned()),
    };
    let mut driver =
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor);

    let error = driver.orders().expect_err("duplicate identity must block");
    assert!(
        error
            .to_string()
            .contains("ambiguous duplicate order identities"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn terminal_preopen_summary_detail_mismatch_fails_before_quarantine() -> Result<()> {
    let order_region = format!(
        "{}{}{}{}{}",
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]"),
        order_row("18:27:29", "全部撤单", 364)
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &order_region),
        detail_xml().replacen("002256", "002257", 1),
    ])));
    let executor = ScriptedAdb {
        snapshots,
        commands: Rc::new(RefCell::new(Vec::new())),
        device_time: Some("2026-08-25T08:01:00+0800".to_owned()),
    };
    let mut driver =
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor);

    let error = driver.orders().expect_err("changed detail must block");
    assert!(
        error
            .to_string()
            .contains("order row changed while reading its identity"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn reviewed_terminal_previous_session_row_is_quarantined_after_open() -> Result<()> {
    let order_region = format!(
        "{}{}{}{}{}",
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]"),
        order_row("18:27:29", "全部撤单", 364)
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &order_region),
        detail_xml(),
    ])));
    let executor = ScriptedAdb {
        snapshots,
        commands: Rc::new(RefCell::new(Vec::new())),
        device_time: Some("2026-08-25T09:30:00+0800".to_owned()),
    };
    let mut driver =
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor);

    assert!(driver.orders()?.rows.is_empty());
    Ok(())
}

#[test]
fn scripted_driver_reads_orders_before_time_only_fills() -> Result<()> {
    let order_region = format!(
        "{}{}{}{}{}",
        node("委托时间", "", "", "[0,289][220,343]"),
        node("委托/均价", "", "", "[220,289][440,343]"),
        node("委托/成交", "", "", "[440,289][660,343]"),
        node("状态", "", "", "[660,289][880,343]"),
        order_row_with_filled("00:00:00", "全部成交", 364, 700)
    );
    let fill_region = format!(
        "{}{}{}{}<node text=\"\" resource-id=\"{PACKAGE}:id/recyclerview_id\" class=\"androidx.recyclerview.widget.RecyclerView\" package=\"{PACKAGE}\" content-desc=\"\" clickable=\"false\" bounds=\"[0,622][1080,2155]\">{}</node>",
        node("成交时间", "", "", "[0,516][250,621]"),
        node("成交价", "", "", "[250,516][500,621]"),
        node("成交量", "", "", "[500,516][750,621]"),
        node("成交额", "", "", "[750,516][1000,621]"),
        fill_row("00:02:12", 700, 622)
    );
    let snapshots = Rc::new(RefCell::new(VecDeque::from([
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &order_region),
        detail_xml(),
        form_xml("模拟炒股", "**0000", ""),
        form_xml("模拟炒股", "**0000", &fill_region),
    ])));
    let commands = Rc::new(RefCell::new(Vec::new()));
    let executor = ScriptedAdb {
        snapshots,
        commands,
        device_time: None,
    };
    let mut driver =
        AndroidThsSimulationUiDriver::with_executor(AndroidThsConfig::default(), executor);
    let orders = driver.orders()?;
    let fills = driver.fills()?.records()?;
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].contract_id, orders.records()?[0].contract_id);
    assert_eq!(fills[0].fill_date, "2026-08-25");
    Ok(())
}
