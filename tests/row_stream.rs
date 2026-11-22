use anyhow::Result;
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use fletch::{FletchRowStreamBuilder, FletchWorkspace, RowValue};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn parquet_files(dir: &Path, run_id: &str, stream_name: &str) -> Result<Vec<PathBuf>> {
    let data_dir = dir.join("runs").join(run_id).join(stream_name);
    let mut files = Vec::new();
    if data_dir.exists() {
        for entry in std::fs::read_dir(&data_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn read_single_file(dir: &Path, run_id: &str, stream_name: &str) -> Result<(PathBuf, RecordBatch)> {
    let files = parquet_files(dir, run_id, stream_name)?;
    assert_eq!(files.len(), 1, "a row stream writes exactly one file");
    let path = files[0].clone();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?.build()?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    let schema = batches[0].schema();
    let batch = arrow::compute::concat_batches(&schema, &batches)?;
    Ok((path, batch))
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column `{name}`"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column `{name}` has an unexpected type"))
}

#[tokio::test]
async fn rows_have_declared_columns_and_no_timestamp_key() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<u64>("ticks")?
        .build("run_001")
        .await?;
    assert_eq!(stream.columns().collect::<Vec<_>>(), ["source", "ticks"]);
    stream.append(&[1_u8.into(), 10_u64.into()])?;
    stream.close()?;

    let (_, batch) = read_single_file(dir.path(), "run_001", "DaqRecords")?;
    let names = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(names, ["source", "ticks"]);
    assert_eq!(batch.num_rows(), 1);
    Ok(())
}

#[tokio::test]
async fn rows_keep_append_order_without_merging_or_sorting() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<u64>("ticks")?
        .column::<f64>("value")?
        .build("run_001")
        .await?;

    // Two sources on their own clocks, interleaved, with repeated and
    // decreasing ticks: a wide stream would merge and sort these.
    let rows = [
        (1_u8, 500_u64, 1.0),
        (2, 10, 2.0),
        (1, 500, 3.0),
        (2, 5, 4.0),
        (1, 100, 5.0),
    ];
    for (source, ticks, value) in rows {
        stream.append(&[source.into(), ticks.into(), value.into()])?;
    }
    stream.close()?;

    let (_, batch) = read_single_file(dir.path(), "run_001", "DaqRecords")?;
    let sources = column::<UInt8Array>(&batch, "source");
    let ticks = column::<UInt64Array>(&batch, "ticks");
    let values = column::<Float64Array>(&batch, "value");
    let written = (0..batch.num_rows())
        .map(|i| (sources.value(i), ticks.value(i), values.value(i)))
        .collect::<Vec<_>>();
    assert_eq!(written, rows);
    Ok(())
}

#[tokio::test]
async fn order_is_kept_across_batch_boundaries() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u64>("sequence")?
        .column::<i64>("ticks")?
        .batch_capacity(3)?
        .build("run_001")
        .await?;

    // Ticks fall across batches, so per-batch sorting would reorder them.
    let rows = (0..10_u64)
        .map(|seq| (seq, 100 - seq as i64))
        .collect::<Vec<_>>();
    for &(sequence, ticks) in &rows {
        stream.append(&[sequence.into(), ticks.into()])?;
    }
    stream.close()?;

    let (_, batch) = read_single_file(dir.path(), "run_001", "DaqRecords")?;
    let sequences = column::<UInt64Array>(&batch, "sequence");
    let ticks = column::<Int64Array>(&batch, "ticks");
    let written = (0..batch.num_rows())
        .map(|i| (sequences.value(i), ticks.value(i)))
        .collect::<Vec<_>>();
    assert_eq!(written, rows);
    Ok(())
}

#[tokio::test]
async fn nulls_are_written_per_column() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<String>("channel")?
        .column::<i64>("raw")?
        .column::<f64>("value")?
        .build("run_001")
        .await?;

    stream.append(&["ain0".into(), 1024_i64.into(), Some(1.25_f64).into()])?;
    stream.append(&["din3".into(), 1_i64.into(), None::<f64>.into()])?;
    stream.append(&[RowValue::Null, RowValue::Null, RowValue::Null])?;
    stream.close()?;

    let (_, batch) = read_single_file(dir.path(), "run_001", "DaqRecords")?;
    assert_eq!(batch.num_rows(), 3);
    for field in batch.schema().fields() {
        assert!(field.is_nullable(), "row stream columns are nullable");
    }
    let channels = column::<StringArray>(&batch, "channel");
    let raws = column::<Int64Array>(&batch, "raw");
    let values = column::<Float64Array>(&batch, "value");
    assert_eq!(channels.value(0), "ain0");
    assert_eq!(raws.value(0), 1024);
    assert_eq!(values.value(0), 1.25);
    assert_eq!(channels.value(1), "din3");
    assert_eq!(raws.value(1), 1);
    assert!(values.is_null(1));
    assert!(channels.is_null(2));
    assert!(raws.is_null(2));
    assert!(values.is_null(2));
    Ok(())
}

#[tokio::test]
async fn every_supported_column_type_round_trips() -> Result<()> {
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
        .column::<f32>("f32")?
        .column::<f64>("f64")?
        .column::<String>("string")?
        .build("run_001")
        .await?;

    let label = String::from("timing-locked");
    stream.append(&[
        true.into(),
        u8::MAX.into(),
        u16::MAX.into(),
        u32::MAX.into(),
        u64::MAX.into(),
        i32::MIN.into(),
        i64::MIN.into(),
        1.5_f32.into(),
        (-2.25_f64).into(),
        (&label).into(),
    ])?;
    stream.append(&[RowValue::Null; 10])?;
    stream.close()?;

    let (_, batch) = read_single_file(dir.path(), "run_001", "AllTypes")?;
    let types = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        types,
        [
            DataType::Boolean,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
        ]
    );
    assert!(column::<BooleanArray>(&batch, "bool").value(0));
    assert_eq!(column::<UInt8Array>(&batch, "u8").value(0), u8::MAX);
    assert_eq!(column::<UInt16Array>(&batch, "u16").value(0), u16::MAX);
    assert_eq!(column::<UInt32Array>(&batch, "u32").value(0), u32::MAX);
    assert_eq!(column::<UInt64Array>(&batch, "u64").value(0), u64::MAX);
    assert_eq!(column::<Int32Array>(&batch, "i32").value(0), i32::MIN);
    assert_eq!(column::<Int64Array>(&batch, "i64").value(0), i64::MIN);
    assert_eq!(column::<Float32Array>(&batch, "f32").value(0), 1.5);
    assert_eq!(column::<Float64Array>(&batch, "f64").value(0), -2.25);
    assert_eq!(
        column::<StringArray>(&batch, "string").value(0),
        "timing-locked"
    );
    for index in 0..batch.num_columns() {
        assert!(batch.column(index).is_null(1));
    }
    Ok(())
}

#[tokio::test]
async fn file_metadata_and_layout_match_wide_streams() -> Result<()> {
    let dir = tempdir()?;
    let run_id = "run_meta";
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u64>("ticks")?
        .metadata("modulo.test_id", "daq-v1")
        .metadata_pairs([
            ("modulo.device_id", "dx0-001"),
            ("fletch.run_id", "spoofed_run"),
        ])
        .build(run_id)
        .await?;
    stream.append(&[7_u64.into()])?;
    stream.close()?;

    let (path, _) = read_single_file(dir.path(), run_id, "DaqRecords")?;
    assert_eq!(
        path.parent().unwrap(),
        dir.path().join("runs").join(run_id).join("DaqRecords")
    );

    let reader = SerializedFileReader::new(File::open(path)?)?;
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("metadata should be present");
    let pairs = metadata
        .iter()
        .map(|entry| {
            (
                entry.key.as_str(),
                entry.value.as_deref().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(pairs.get("fletch.run_id"), Some(&run_id));
    assert_eq!(pairs.get("fletch.stream_name"), Some(&"DaqRecords"));
    assert!(pairs.contains_key("fletch.created_at_ns"));
    assert_eq!(pairs.get("modulo.test_id"), Some(&"daq-v1"));
    assert_eq!(pairs.get("modulo.device_id"), Some(&"dx0-001"));
    Ok(())
}

#[tokio::test]
async fn rejected_rows_leave_the_stream_unchanged() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<u64>("ticks")?
        .build("run_001")
        .await?;

    stream.append(&[1_u8.into(), 1_u64.into()])?;
    let too_short = stream.append(&[2_u8.into()]);
    assert!(too_short.is_err());
    // The first value matches, the second does not: nothing may be written.
    let wrong_type = stream.append(&[3_u8.into(), 3_i64.into()]);
    let message = wrong_type.unwrap_err().to_string();
    assert!(message.contains("ticks"), "{message}");
    stream.append(&[4_u8.into(), 4_u64.into()])?;
    stream.close()?;

    let (_, batch) = read_single_file(dir.path(), "run_001", "DaqRecords")?;
    let sources = column::<UInt8Array>(&batch, "source");
    assert_eq!(sources.values().to_vec(), [1, 4]);
    Ok(())
}

#[tokio::test]
async fn builder_rejects_invalid_definitions() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;

    let duplicate = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<u16>("source");
    assert!(duplicate.is_err());

    let empty = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .build("run_001")
        .await;
    assert!(empty.is_err());

    let zero_capacity = FletchRowStreamBuilder::new(&workspace, "DaqRecords").batch_capacity(0);
    assert!(zero_capacity.is_err());

    let unsafe_run = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .build("../escape")
        .await;
    assert!(unsafe_run.is_err());
    Ok(())
}

#[tokio::test]
async fn empty_row_stream_closes_without_a_file() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let stream = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .build("run_001")
        .await?;
    stream.close()?;
    assert!(parquet_files(dir.path(), "run_001", "DaqRecords")?.is_empty());
    Ok(())
}
