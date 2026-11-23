use fletch::{FletchEnum, FletchSchema, FletchViewBuilder, FletchWorkspace, Stream, View};
use tempfile::tempdir;

/// The DAQ channels, stored by their catalog names.
#[derive(Clone, Copy, Debug, PartialEq, FletchEnum)]
#[fletch(rename_all = "SCREAMING_SNAKE_CASE")]
enum Channel {
    AnIn0,
    DigIn3,
}

/// One DAQ record: the source module, the channel, that module's tick count
/// and the raw reading. Each field is a column.
#[derive(Debug, FletchSchema)]
#[fletch(stream = "DaqRecords")]
struct DaqRecord {
    source: u8,
    channel: Channel,
    ticks: u64,
    raw: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let run_id = "run_001";
    println!("Logging DAQ records to: {}", dir.path().display());

    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut records = Stream::<DaqRecord>::builder(&workspace)
        .metadata("modulo.test_id", "daq-example")
        .build(run_id)
        .await?;

    // Two modules, each counting its own ticks at its own rate. Their records
    // arrive interleaved; a row stream keeps every one as written.
    for i in 0..100_000_u64 {
        records.write(&DaqRecord {
            source: 1,
            channel: Channel::AnIn0,
            ticks: i * 10,
            raw: (i % 4096) as i64,
        })?;
        if i % 10 == 0 {
            records.write(&DaqRecord {
                source: 2,
                channel: Channel::DigIn3,
                ticks: i,
                raw: ((i / 10) % 2) as i64,
            })?;
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

    let edges = View::<DaqRecord>::builder(&workspace)
        .run_id(run_id)
        .filter(DaqRecord::CHANNEL.eq(Channel::DigIn3))
        .filter(DaqRecord::TICKS.between(0, 100))
        .order_by([DaqRecord::TICKS.desc()])
        .build()?
        .rows()?;
    println!(
        "--- {} on source 2, newest first ---",
        Channel::DigIn3.name()
    );
    for edge in edges {
        println!("{edge:?}");
    }

    Ok(())
}
