#![cfg(feature = "view")]

use anyhow::Result;
use fletch::{
    FletchEnum, FletchRowStreamBuilder, FletchSchema, FletchViewBuilder, FletchWorkspace, Stream,
    View,
};
use polars::prelude::{col, lit};
use tempfile::tempdir;

#[derive(Clone, Copy, Debug, PartialEq, FletchEnum)]
#[fletch(rename_all = "SCREAMING_SNAKE_CASE")]
enum Channel {
    AnIn1,
    AnIn2,
    #[fletch(rename = "DIG_3")]
    DigIn3,
}

#[derive(Clone, Debug, PartialEq, FletchSchema)]
#[fletch(stream = "DaqRecords")]
struct DaqRecord {
    source: u8,
    channel: Channel,
    ticks: u64,
    raw: i64,
    label: Option<String>,
}

fn record(source: u8, channel: Channel, ticks: u64, raw: i64, label: Option<&str>) -> DaqRecord {
    DaqRecord {
        source,
        channel,
        ticks,
        raw,
        label: label.map(str::to_string),
    }
}

/// Two sources on their own clocks, interleaved, with a repeated tick on
/// source 1 and ticks that run backwards on source 2.
fn records() -> Vec<DaqRecord> {
    vec![
        record(1, Channel::AnIn1, 500, 10, Some("first")),
        record(2, Channel::DigIn3, 40, 20, None),
        record(1, Channel::AnIn2, 500, 30, None),
        record(2, Channel::DigIn3, 10, 40, Some("edge")),
        record(1, Channel::AnIn1, 100, 50, None),
    ]
}

async fn write_records(workspace: &FletchWorkspace, run_id: &str) -> Result<()> {
    let mut stream = Stream::<DaqRecord>::builder(workspace)
        .build(run_id)
        .await?;
    for record in records() {
        stream.write(&record)?;
    }
    stream.close()
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_view_round_trips_rows_in_append_order() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_records(&workspace, "run_a").await?;

    let rows = View::<DaqRecord>::builder(&workspace)
        .run_id("run_a")
        .build()?
        .rows()?;
    assert_eq!(rows, records());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_view_filters_and_orders_by_typed_columns() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_records(&workspace, "run_a").await?;

    let by_source_then_ticks = View::<DaqRecord>::builder(&workspace)
        .run_id("run_a")
        .order_by([DaqRecord::SOURCE.asc(), DaqRecord::TICKS.asc()])
        .build()?
        .rows()?;
    let raws = by_source_then_ticks
        .iter()
        .map(|r| r.raw)
        .collect::<Vec<_>>();
    // Source 1's two rows at tick 500 keep the order they were appended in.
    assert_eq!(raws, [50, 10, 30, 40, 20]);

    let newest_first = View::<DaqRecord>::builder(&workspace)
        .run_id("run_a")
        .filter(DaqRecord::SOURCE.eq(1))
        .filter(DaqRecord::TICKS.between(100, 500))
        .order_by([DaqRecord::TICKS.desc()])
        .build()?
        .rows()?;
    let raws = newest_first.iter().map(|r| r.raw).collect::<Vec<_>>();
    assert_eq!(raws, [10, 30, 50]);

    let combined = View::<DaqRecord>::builder(&workspace)
        .run_id("run_a")
        .filter(
            DaqRecord::TICKS
                .lt(50)
                .or(DaqRecord::RAW.ge(50))
                .and(!DaqRecord::RAW.eq(20)),
        )
        .order_by([DaqRecord::RAW])
        .build()?
        .rows()?;
    let raws = combined.iter().map(|r| r.raw).collect::<Vec<_>>();
    assert_eq!(raws, [40, 50]);

    let some_raws = View::<DaqRecord>::builder(&workspace)
        .run_id("run_a")
        .filter(DaqRecord::RAW.is_in([20, 50, 99]) & DaqRecord::SOURCE.ne(2))
        .build()?
        .rows()?;
    assert_eq!(some_raws, [record(1, Channel::AnIn1, 100, 50, None)]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_view_filters_by_enum_and_nulls() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_records(&workspace, "run_a").await?;
    let view = || View::<DaqRecord>::builder(&workspace).run_id("run_a");

    let digital = view()
        .filter(DaqRecord::CHANNEL.eq(Channel::DigIn3))
        .build()?
        .rows()?;
    assert_eq!(digital.iter().map(|r| r.raw).collect::<Vec<_>>(), [20, 40]);

    let analog = view()
        .filter(DaqRecord::CHANNEL.is_in([Channel::AnIn1, Channel::AnIn2]))
        .filter(DaqRecord::CHANNEL.ne(Channel::AnIn2))
        .build()?
        .rows()?;
    assert_eq!(analog.iter().map(|r| r.raw).collect::<Vec<_>>(), [10, 50]);

    let unlabeled = view().filter(DaqRecord::LABEL.is_null()).build()?.rows()?;
    assert_eq!(
        unlabeled.iter().map(|r| r.raw).collect::<Vec<_>>(),
        [20, 30, 50]
    );
    assert!(unlabeled.iter().all(|r| r.label.is_none()));

    let labeled = view()
        .filter(DaqRecord::LABEL.is_not_null())
        .filter(DaqRecord::LABEL.ne("edge"))
        .build()?
        .rows()?;
    assert_eq!(labeled, [record(1, Channel::AnIn1, 500, 10, Some("first"))]);

    let no_channels = view()
        .filter(DaqRecord::CHANNEL.is_in(Vec::<Channel>::new()))
        .build()?
        .rows()?;
    assert!(no_channels.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_view_stores_enums_as_their_names() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_records(&workspace, "run_a").await?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_a")
        .build()?
        .collect()?;
    let channels = df
        .column("channel")?
        .str()?
        .into_iter()
        .map(|channel| channel.unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(channels, ["AN_IN1", "DIG_3", "AN_IN2", "DIG_3", "AN_IN1"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_expr_takes_a_raw_polars_expression() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_records(&workspace, "run_a").await?;

    let rows = View::<DaqRecord>::builder(&workspace)
        .run_id("run_a")
        .filter_expr((col("raw") % lit(20_i64)).eq(lit(0_i64)))
        .build()?
        .rows()?;
    assert_eq!(rows.iter().map(|r| r.raw).collect::<Vec<_>>(), [20, 40]);

    // A typed predicate also works in a string-based view.
    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_a")
        .filter(DaqRecord::SOURCE.eq(2).into())
        .build()?
        .collect()?;
    assert_eq!(df.height(), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_view_without_stream_attribute_takes_a_name_and_reads_every_run() -> Result<()> {
    #[derive(Debug, PartialEq, FletchSchema)]
    struct Tick {
        ticks: u64,
    }

    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    for (run_id, ticks) in [("run_b", 2), ("run_a", 1)] {
        let mut stream = Stream::<Tick>::builder_named(&workspace, "Ticks")
            .build(run_id)
            .await?;
        stream.write(&Tick { ticks })?;
        stream.close()?;
    }

    let rows = View::<Tick>::builder_named(&workspace, "Ticks")
        .order_by([Tick::TICKS])
        .build()?
        .rows()?;
    assert_eq!(rows, [Tick { ticks: 1 }, Tick { ticks: 2 }]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_view_rejects_a_stream_that_does_not_match_the_struct() -> Result<()> {
    #[derive(Debug, FletchSchema)]
    struct Declared {
        source: u8,
        ticks: u64,
    }

    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut wrong_type = FletchRowStreamBuilder::new(&workspace, "WrongType")
        .column::<u8>("source")?
        .column::<i64>("ticks")?
        .build("run_a")
        .await?;
    wrong_type.append(&[1_u8.into(), 5_i64.into()])?;
    wrong_type.close()?;
    let mut missing = FletchRowStreamBuilder::new(&workspace, "Missing")
        .column::<u8>("source")?
        .build("run_a")
        .await?;
    missing.append(&[1_u8.into()])?;
    missing.close()?;
    let mut extra = FletchRowStreamBuilder::new(&workspace, "Extra")
        .column::<u8>("source")?
        .column::<u64>("ticks")?
        .column::<String>("note")?
        .build("run_a")
        .await?;
    extra.append(&[1_u8.into(), 5_u64.into(), "x".into()])?;
    extra.close()?;

    let error = |stream: &str| {
        View::<Declared>::builder_named(&workspace, stream)
            .run_id("run_a")
            .build()
            .err()
            .expect("a mismatched schema must be rejected")
            .to_string()
    };
    let wrong_type = error("WrongType");
    assert!(
        wrong_type.contains("`ticks`") && wrong_type.contains("i64"),
        "{wrong_type}"
    );
    let missing = error("Missing");
    assert!(missing.contains("no column `ticks`"), "{missing}");
    let extra = error("Extra");
    assert!(extra.contains("`note`"), "{extra}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_reads_reject_nulls_in_required_fields_and_unknown_enum_names() -> Result<()> {
    #[derive(Debug, FletchSchema)]
    struct Required {
        source: u8,
        channel: Channel,
    }

    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut nulls = FletchRowStreamBuilder::new(&workspace, "Nulls")
        .column::<u8>("source")?
        .column::<String>("channel")?
        .build("run_a")
        .await?;
    nulls.append(&[None::<u8>.into(), "AN_IN1".into()])?;
    nulls.close()?;
    let mut unknown = FletchRowStreamBuilder::new(&workspace, "Unknown")
        .column::<u8>("source")?
        .column::<Channel>("channel")?
        .build("run_a")
        .await?;
    unknown.append(&[1_u8.into(), Channel::AnIn1.into()])?;
    unknown.append(&[2_u8.into(), "AN_IN9".into()])?;
    unknown.close()?;

    let error = |stream: &str| {
        View::<Required>::builder_named(&workspace, stream)
            .run_id("run_a")
            .build()
            .and_then(|view| view.rows())
            .expect_err("the read must fail")
            .to_string()
    };
    let null = error("Nulls");
    assert!(null.contains("`source`") && null.contains("null"), "{null}");
    let unknown = error("Unknown");
    assert!(
        unknown.contains("`channel`") && unknown.contains("AN_IN9"),
        "{unknown}"
    );
    Ok(())
}

#[derive(Debug, PartialEq, FletchSchema)]
#[fletch(stream = "NonFloat")]
struct NonFloat {
    flag: bool,
    small: u8,
    port: u16,
    count: u32,
    ticks: u64,
    offset: i32,
    raw: i64,
    text: String,
    channel: Channel,
    maybe_flag: Option<bool>,
    maybe_small: Option<u8>,
    maybe_port: Option<u16>,
    maybe_count: Option<u32>,
    maybe_ticks: Option<u64>,
    maybe_offset: Option<i32>,
    maybe_raw: Option<i64>,
    maybe_text: Option<String>,
    maybe_channel: Option<Channel>,
}

fn non_float(present: bool) -> NonFloat {
    NonFloat {
        flag: present,
        small: u8::MAX,
        port: u16::MAX,
        count: u32::MAX,
        ticks: u64::MAX,
        offset: i32::MIN,
        raw: i64::MIN,
        text: "timing-locked".to_string(),
        channel: Channel::DigIn3,
        maybe_flag: present.then_some(true),
        maybe_small: present.then_some(1),
        maybe_port: present.then_some(2),
        maybe_count: present.then_some(3),
        maybe_ticks: present.then_some(4),
        maybe_offset: present.then_some(-5),
        maybe_raw: present.then_some(-6),
        maybe_text: present.then(|| "seven".to_string()),
        maybe_channel: present.then_some(Channel::AnIn2),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_non_float_type_and_option_round_trips_through_a_typed_view() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = Stream::<NonFloat>::builder(&workspace)
        .build("run_a")
        .await?;
    stream.write(&non_float(true))?;
    stream.write(&non_float(false))?;
    stream.close()?;

    let rows = View::<NonFloat>::builder(&workspace)
        .run_id("run_a")
        .build()?
        .rows()?;
    assert_eq!(rows, [non_float(true), non_float(false)]);
    Ok(())
}

#[derive(Debug, PartialEq, FletchSchema)]
#[fletch(stream = "Floats")]
struct Floats {
    gain: f32,
    value: f64,
    maybe_gain: Option<f32>,
    maybe_value: Option<f64>,
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "polars cannot read parquet 60's IEEE_754_TOTAL_ORDER column order on float columns (pola-rs/polars#27893); re-enable with corebit-instruments/fletch#11"]
async fn float_types_round_trip_through_a_typed_view() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let written = [
        Floats {
            gain: 0.5,
            value: -2.25,
            maybe_gain: Some(1.5),
            maybe_value: None,
        },
        Floats {
            gain: f32::MAX,
            value: f64::MIN,
            maybe_gain: None,
            maybe_value: Some(3.0),
        },
    ];
    let mut stream = Stream::<Floats>::builder(&workspace).build("run_a").await?;
    for value in &written {
        stream.write(value)?;
    }
    stream.close()?;

    let rows = View::<Floats>::builder(&workspace)
        .run_id("run_a")
        .build()?
        .rows()?;
    assert_eq!(rows, written);
    Ok(())
}
