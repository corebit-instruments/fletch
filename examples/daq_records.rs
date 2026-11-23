use fletch::{FletchSchema, FletchViewBuilder, FletchWorkspace, Stream};
use polars::prelude::{col, lit};
use tempfile::tempdir;

/// One DAQ record: the source module, the channel, that module's tick count
/// and the raw reading. Each field is a column.
#[derive(FletchSchema)]
struct DaqRecord {
    source: u8,
    channel: String,
    ticks: u64,
    raw: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let run_id = "run_001";
    println!("Logging DAQ records to: {}", dir.path().display());

    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut records = Stream::<DaqRecord>::builder(&workspace, "DaqRecords")
        .metadata("modulo.test_id", "daq-example")
        .build(run_id)
        .await?;

    // Two modules, each counting its own ticks at its own rate. Their records
    // arrive interleaved; a row stream keeps every one as written. The records
    // are reused: each write borrows their fields into one row.
    let mut analog = DaqRecord {
        source: 1,
        channel: "ain0".to_string(),
        ticks: 0,
        raw: 0,
    };
    let mut digital = DaqRecord {
        source: 2,
        channel: "din3".to_string(),
        ticks: 0,
        raw: 0,
    };
    for i in 0..100_000_u64 {
        analog.ticks = i * 10;
        analog.raw = (i % 4096) as i64;
        records.write(&analog)?;
        if i % 10 == 0 {
            digital.ticks = i;
            digital.raw = ((i / 10) % 2) as i64;
            records.write(&digital)?;
        }
    }
    records.close()?;
    println!("Wrote DAQ records to Parquet.\n");

    let all_sources = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id(run_id)
        .order_by(&["source", "ticks"])
        .build()?
        .collect()?;
    println!("--- Every source, by source then ticks ---");
    println!("{}\n", all_sources.head(Some(10)));

    let digital = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id(run_id)
        .filter(col("source").eq(lit(2_u8)))
        .columns(&["ticks", "raw"])
        .build()?
        .collect()?;
    println!("--- Source 2 only ---");
    println!("{}\n", digital.head(Some(10)));

    Ok(())
}
