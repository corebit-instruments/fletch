use fletch::{FletchRowStreamBuilder, FletchViewBuilder, FletchWorkspace};
use polars::prelude::{col, lit};
use tempfile::tempdir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let run_id = "run_001";
    println!("Logging DAQ records to: {}", dir.path().display());

    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut records = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<String>("channel")?
        .column::<u64>("ticks")?
        .column::<i64>("raw")?
        .metadata("modulo.test_id", "daq-example")
        .build(run_id)
        .await?;

    // Two modules, each counting its own ticks at its own rate. Their records
    // arrive interleaved; a row stream keeps every one as appended.
    for i in 0..100_000_u64 {
        let raw = (i % 4096) as i64;
        records.append(&[1_u8.into(), "ain0".into(), (i * 10).into(), raw.into()])?;
        if i % 10 == 0 {
            let edge = ((i / 10) % 2) as i64;
            records.append(&[2_u8.into(), "din3".into(), i.into(), edge.into()])?;
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
