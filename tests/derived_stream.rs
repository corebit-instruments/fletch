use anyhow::Result;
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use fletch::{FletchSchema, FletchWorkspace, Stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[derive(FletchSchema)]
struct AllTypes {
    flag: bool,
    source: u8,
    port: u16,
    count: u32,
    ticks: u64,
    offset: i32,
    raw: i64,
    gain: f32,
    value: f64,
    channel: String,
    maybe_raw: Option<i64>,
    maybe_value: Option<f64>,
    maybe_label: Option<String>,
}

#[derive(FletchSchema)]
struct DaqRecord {
    source: u8,
    ticks: u64,
    r#type: String,
}

fn single_file(dir: &Path, run_id: &str, stream_name: &str) -> Result<PathBuf> {
    let data_dir = dir.join("runs").join(run_id).join(stream_name);
    let files = std::fs::read_dir(&data_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(files.len(), 1, "a stream writes exactly one file");
    Ok(files[0].clone())
}

fn read_batch(path: &Path) -> Result<RecordBatch> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(arrow::compute::concat_batches(
        &batches[0].schema(),
        &batches,
    )?)
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column `{name}`"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column `{name}` has an unexpected type"))
}

fn all_types(i: u8) -> AllTypes {
    AllTypes {
        flag: i % 2 == 0,
        source: i,
        port: 1000 + u16::from(i),
        count: 10 * u32::from(i),
        ticks: 500 - u64::from(i) * 100,
        offset: -i32::from(i),
        raw: i64::from(i) * 1024,
        gain: f32::from(i) * 0.5,
        value: f64::from(i) * 1.25,
        channel: format!("ain{i}"),
        maybe_raw: (i != 1).then_some(i64::from(i)),
        maybe_value: (i != 2).then_some(f64::from(i)),
        maybe_label: (i == 1).then(|| "only-one".to_string()),
    }
}

#[tokio::test]
async fn derived_stream_writes_one_row_per_value_with_a_column_per_field() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = Stream::<AllTypes>::builder(&workspace, "AllTypes")
        .batch_capacity(2)?
        .build("run_001")
        .await?;
    let names = [
        "flag",
        "source",
        "port",
        "count",
        "ticks",
        "offset",
        "raw",
        "gain",
        "value",
        "channel",
        "maybe_raw",
        "maybe_value",
        "maybe_label",
    ];
    assert_eq!(stream.columns().collect::<Vec<_>>(), names);
    // Ticks decrease across values and batches: rows keep write order.
    for i in 0..4 {
        stream.write(&all_types(i))?;
    }
    stream.close()?;

    let batch = read_batch(&single_file(dir.path(), "run_001", "AllTypes")?)?;
    let schema = batch.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type().clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        fields,
        [
            ("flag", DataType::Boolean),
            ("source", DataType::UInt8),
            ("port", DataType::UInt16),
            ("count", DataType::UInt32),
            ("ticks", DataType::UInt64),
            ("offset", DataType::Int32),
            ("raw", DataType::Int64),
            ("gain", DataType::Float32),
            ("value", DataType::Float64),
            ("channel", DataType::Utf8),
            ("maybe_raw", DataType::Int64),
            ("maybe_value", DataType::Float64),
            ("maybe_label", DataType::Utf8),
        ]
    );
    assert!(schema.fields().iter().all(|field| field.is_nullable()));
    assert_eq!(batch.num_rows(), 4);

    for i in 0..4_u8 {
        let row = usize::from(i);
        let expected = all_types(i);
        assert_eq!(
            column::<BooleanArray>(&batch, "flag").value(row),
            expected.flag
        );
        assert_eq!(
            column::<UInt8Array>(&batch, "source").value(row),
            expected.source
        );
        assert_eq!(
            column::<UInt16Array>(&batch, "port").value(row),
            expected.port
        );
        assert_eq!(
            column::<UInt32Array>(&batch, "count").value(row),
            expected.count
        );
        assert_eq!(
            column::<UInt64Array>(&batch, "ticks").value(row),
            expected.ticks
        );
        assert_eq!(
            column::<Int32Array>(&batch, "offset").value(row),
            expected.offset
        );
        assert_eq!(column::<Int64Array>(&batch, "raw").value(row), expected.raw);
        assert_eq!(
            column::<Float32Array>(&batch, "gain").value(row),
            expected.gain
        );
        assert_eq!(
            column::<Float64Array>(&batch, "value").value(row),
            expected.value
        );
        assert_eq!(
            column::<StringArray>(&batch, "channel").value(row),
            expected.channel
        );
    }

    let maybe_raw = column::<Int64Array>(&batch, "maybe_raw");
    let maybe_value = column::<Float64Array>(&batch, "maybe_value");
    let maybe_label = column::<StringArray>(&batch, "maybe_label");
    let nulls = |array: &dyn Array| (0..4).map(|i| array.is_null(i)).collect::<Vec<_>>();
    assert_eq!(nulls(maybe_raw), [false, true, false, false]);
    assert_eq!(nulls(maybe_value), [false, false, true, false]);
    assert_eq!(nulls(maybe_label), [true, false, true, true]);
    assert_eq!(maybe_raw.value(3), 3);
    assert_eq!(maybe_value.value(3), 3.0);
    assert_eq!(maybe_label.value(1), "only-one");
    Ok(())
}

#[tokio::test]
async fn derived_stream_writes_metadata_and_raw_field_names() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let mut stream = Stream::<DaqRecord>::builder(&workspace, "DaqRecords")
        .metadata("modulo.test_id", "daq-v1")
        .metadata_pairs([("fletch.run_id", "spoofed_run")])
        .build("run_meta")
        .await?;
    assert_eq!(
        stream.columns().collect::<Vec<_>>(),
        ["source", "ticks", "type"]
    );
    stream.write(&DaqRecord {
        source: 2,
        ticks: 7,
        r#type: "din".to_string(),
    })?;
    stream.close()?;

    let path = single_file(dir.path(), "run_meta", "DaqRecords")?;
    let reader = SerializedFileReader::new(File::open(&path)?)?;
    let metadata = reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .expect("metadata should be present")
        .iter()
        .map(|entry| (entry.key.clone(), entry.value.clone().unwrap_or_default()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(metadata["fletch.run_id"], "run_meta");
    assert_eq!(metadata["fletch.stream_name"], "DaqRecords");
    assert_eq!(metadata["modulo.test_id"], "daq-v1");

    let batch = read_batch(&path)?;
    assert_eq!(column::<StringArray>(&batch, "type").value(0), "din");
    Ok(())
}

#[tokio::test]
async fn empty_derived_stream_closes_without_a_file() -> Result<()> {
    let dir = tempdir()?;
    let workspace = FletchWorkspace::builder().root(dir.path()).build()?;
    let stream = Stream::<DaqRecord>::builder(&workspace, "DaqRecords")
        .build("run_001")
        .await?;
    stream.close()?;
    let data_dir = dir.path().join("runs").join("run_001").join("DaqRecords");
    assert_eq!(std::fs::read_dir(data_dir)?.count(), 0);
    Ok(())
}
