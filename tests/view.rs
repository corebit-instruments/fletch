#![cfg(feature = "view")]

use anyhow::Result;
use fletch::{
    FletchRowStream, FletchRowStreamBuilder, FletchViewBuilder, FletchWorkspace, RowValue,
};
use polars::prelude::{DataFrame, col, lit};
use tempfile::tempdir;

async fn daq_stream(workspace: &FletchWorkspace, run_id: &str) -> Result<FletchRowStream> {
    FletchRowStreamBuilder::new(workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<String>("channel")?
        .column::<u64>("ticks")?
        .column::<i64>("raw")?
        .build(run_id)
        .await
}

/// Two sources on their own clocks, interleaved, with a repeated tick on
/// source 1 and ticks that run backwards on source 2.
async fn write_interleaved_records(workspace: &FletchWorkspace, run_id: &str) -> Result<()> {
    let mut stream = daq_stream(workspace, run_id).await?;
    for (source, channel, ticks, raw) in [
        (1_u8, "ain0", 500_u64, 10_i64),
        (2, "din3", 40, 20),
        (1, "ain1", 500, 30),
        (2, "din3", 10, 40),
        (1, "ain0", 100, 50),
    ] {
        stream.append(&[source.into(), channel.into(), ticks.into(), raw.into()])?;
    }
    stream.close()
}

fn column_names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect()
}

fn source_ticks_raw(df: &DataFrame) -> Result<Vec<(u8, u64, i64)>> {
    let sources = df.column("source")?.u8()?;
    let ticks = df.column("ticks")?.u64()?;
    let raws = df.column("raw")?.i64()?;
    Ok((0..df.height())
        .map(|i| {
            (
                sources.get(i).unwrap(),
                ticks.get(i).unwrap(),
                raws.get(i).unwrap(),
            )
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn view_keeps_append_order_and_every_declared_column_by_default() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_interleaved_records(&workspace, "run_a").await?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_a")
        .build()?
        .collect()?;

    assert_eq!(
        column_names(&df),
        ["run_id", "source", "channel", "ticks", "raw"]
    );
    assert_eq!(
        source_ticks_raw(&df)?,
        [
            (1, 500, 10),
            (2, 40, 20),
            (1, 500, 30),
            (2, 10, 40),
            (1, 100, 50)
        ]
    );
    let run_ids = df.column("run_id")?.str()?;
    assert!(run_ids.into_iter().all(|run_id| run_id == Some("run_a")));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn view_orders_by_source_then_ticks_and_ties_keep_append_order() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_interleaved_records(&workspace, "run_a").await?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_a")
        .order_by(&["source", "ticks"])
        .build()?
        .collect()?;

    // Source 1's two rows at tick 500 keep the order they were appended in.
    assert_eq!(
        source_ticks_raw(&df)?,
        [
            (1, 100, 50),
            (1, 500, 10),
            (1, 500, 30),
            (2, 10, 40),
            (2, 40, 20)
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn view_filters_and_selects_declared_columns() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_interleaved_records(&workspace, "run_a").await?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_a")
        .filter(col("source").eq(lit(1_u8)))
        .filter(col("channel").eq(lit("ain0")))
        .order_by(&["ticks"])
        .columns(&["ticks", "raw"])
        .build()?
        .collect()?;

    assert_eq!(column_names(&df), ["run_id", "ticks", "raw"]);
    let ticks = df.column("ticks")?.u64()?;
    let raws = df.column("raw")?.i64()?;
    let rows = (0..df.height())
        .map(|i| (ticks.get(i).unwrap(), raws.get(i).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(rows, [(100, 50), (500, 10)]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn view_without_run_id_reads_every_run() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut run_b = daq_stream(&workspace, "run_b").await?;
    run_b.append(&[2_u8.into(), "din3".into(), 7_u64.into(), 2_i64.into()])?;
    run_b.close()?;
    let mut run_a = daq_stream(&workspace, "run_a").await?;
    run_a.append(&[1_u8.into(), "ain0".into(), 9_u64.into(), 1_i64.into()])?;
    run_a.close()?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .order_by(&["run_id"])
        .build()?
        .collect()?;

    let run_ids = df.column("run_id")?.str()?;
    let rows = (0..df.height())
        .map(|i| run_ids.get(i).unwrap().to_string())
        .zip(source_ticks_raw(&df)?)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            ("run_a".to_string(), (1, 9, 1)),
            ("run_b".to_string(), (2, 7, 2))
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn view_rejects_missing_streams_and_unsafe_names() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    write_interleaved_records(&workspace, "run_a").await?;

    let missing_stream = FletchViewBuilder::new(&workspace, "Missing").build();
    assert!(missing_stream.is_err());
    let missing_run = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_z")
        .build();
    assert!(missing_run.is_err());
    let unsafe_stream = FletchViewBuilder::new(&workspace, "../DaqRecords").build();
    assert!(unsafe_stream.is_err());
    let unsafe_run = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("../run_a")
        .build();
    assert!(unsafe_run.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn view_reads_every_non_float_column_type() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "AllTypes")
        .column::<bool>("bool")?
        .column::<u8>("u8")?
        .column::<u16>("u16")?
        .column::<u32>("u32")?
        .column::<u64>("u64")?
        .column::<i32>("i32")?
        .column::<i64>("i64")?
        .column::<String>("string")?
        .build("run_a")
        .await?;
    stream.append(&[
        true.into(),
        u8::MAX.into(),
        u16::MAX.into(),
        u32::MAX.into(),
        u64::MAX.into(),
        i32::MIN.into(),
        i64::MIN.into(),
        "timing-locked".into(),
    ])?;
    stream.append(&[RowValue::Null; 8])?;
    stream.close()?;

    let df = FletchViewBuilder::new(&workspace, "AllTypes")
        .run_id("run_a")
        .build()?
        .collect()?;

    assert_eq!(df.column("bool")?.bool()?.get(0), Some(true));
    assert_eq!(df.column("u8")?.u8()?.get(0), Some(u8::MAX));
    assert_eq!(df.column("u16")?.u16()?.get(0), Some(u16::MAX));
    assert_eq!(df.column("u32")?.u32()?.get(0), Some(u32::MAX));
    assert_eq!(df.column("u64")?.u64()?.get(0), Some(u64::MAX));
    assert_eq!(df.column("i32")?.i32()?.get(0), Some(i32::MIN));
    assert_eq!(df.column("i64")?.i64()?.get(0), Some(i64::MIN));
    assert_eq!(df.column("string")?.str()?.get(0), Some("timing-locked"));
    for name in ["bool", "u8", "u16", "u32", "u64", "i32", "i64", "string"] {
        assert_eq!(df.column(name)?.null_count(), 1, "{name}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "polars cannot read parquet 60's IEEE_754_TOTAL_ORDER column order on float columns (pola-rs/polars#27893); re-enable with corebit-instruments/fletch#11"]
async fn view_reads_float_columns() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u64>("ticks")?
        .column::<f32>("gain")?
        .column::<f64>("value")?
        .build("run_a")
        .await?;
    stream.append(&[1_u64.into(), 0.5_f32.into(), 1.25_f64.into()])?;
    stream.close()?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_a")
        .build()?
        .collect()?;

    assert_eq!(df.column("gain")?.f32()?.get(0), Some(0.5));
    assert_eq!(df.column("value")?.f64()?.get(0), Some(1.25));
    Ok(())
}
