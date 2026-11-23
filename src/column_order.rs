//! Parquet 60 writes the IEEE 754 total-order column order (PARQUET-2249) for
//! every FLOAT and DOUBLE column, and has no writer setting to turn it off.
//! Polars' Parquet reader (polars-parquet-format 0.1, used by every Polars
//! release up to 0.55.2) knows only `TYPE_ORDER` and rejects that footer with
//! "Invalid thrift: bad data" (pola-rs/polars#27893). Fletch rewrites those
//! entries to `TYPE_ORDER`, the order every Parquet writer used before
//! PARQUET-2249, so the files stay readable by Polars and every other reader.
//!
//! The rewrite is in place and keeps the footer's length: in the Thrift compact
//! protocol each `ColumnOrder` union is three bytes, a field header naming the
//! union member (`0x1C` for field 1 `TYPE_ORDER`, `0x2C` for field 2
//! `IEEE_754_TOTAL_ORDER`) followed by the member's and the union's stop bytes.
//! Remove this module once the Polars Parquet reader accepts the new order.

use anyhow::{Result, bail};
use parquet::basic::ColumnOrder;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 4] = b"PAR1";
/// Footer length (4 bytes, little-endian) followed by the magic.
const TRAILER_LEN: usize = 8;
const TYPE_ORDER: [u8; 3] = [0x1C, 0x00, 0x00];
const IEEE_754_TOTAL_ORDER: [u8; 3] = [0x2C, 0x00, 0x00];
const INT96_TIMESTAMP_ORDER: [u8; 3] = [0x3C, 0x00, 0x00];
const STRUCT_STOP: u8 = 0x00;
const COMPACT_STRUCT: u8 = 0x0C;

/// Rewrites the IEEE 754 total-order column orders of the closed Parquet file
/// at `path` to `TYPE_ORDER`. `metadata` is what the writer returned on close.
pub(crate) fn write_type_order_for_floats(path: &Path, metadata: &ParquetMetaData) -> Result<()> {
    let Some(orders) = metadata.file_metadata().column_orders() else {
        return Ok(());
    };
    if !orders.contains(&ColumnOrder::IEEE_754_TOTAL_ORDER) {
        return Ok(());
    }

    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    let mut trailer = [0_u8; TRAILER_LEN];
    file.seek(SeekFrom::End(-(TRAILER_LEN as i64)))?;
    file.read_exact(&mut trailer)?;
    if &trailer[4..] != MAGIC {
        bail!("{} does not end with the Parquet magic", path.display());
    }
    let footer_len = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as u64;
    let footer_start = file_len - TRAILER_LEN as u64 - footer_len;
    let mut footer = vec![0_u8; footer_len as usize];
    file.seek(SeekFrom::Start(footer_start))?;
    file.read_exact(&mut footer)?;

    rewrite_footer(&mut footer, orders)?;

    // Check the rewritten footer before it reaches the file.
    let rewritten = ParquetMetaDataReader::decode_metadata(&footer)?;
    if rewritten
        .file_metadata()
        .column_orders()
        .is_some_and(|orders| orders.contains(&ColumnOrder::IEEE_754_TOTAL_ORDER))
    {
        bail!("rewritten Parquet footer still has an IEEE 754 total-order column");
    }

    file.seek(SeekFrom::Start(footer_start))?;
    file.write_all(&footer)?;
    file.sync_all()?;
    Ok(())
}

/// The column orders are FileMetaData's last field, so the footer ends with
/// their list header, one three-byte union per leaf column, and the
/// FileMetaData stop byte. Anything else means the writer's layout changed:
/// the footer is left as it was, and the error reaches the stream's `close`.
fn rewrite_footer(footer: &mut [u8], orders: &[ColumnOrder]) -> Result<()> {
    let mut entries = Vec::with_capacity(orders.len() * 3);
    for order in orders {
        entries.extend_from_slice(match order {
            ColumnOrder::TYPE_DEFINED_ORDER(_) => &TYPE_ORDER,
            ColumnOrder::IEEE_754_TOTAL_ORDER => &IEEE_754_TOTAL_ORDER,
            ColumnOrder::INT96_TIMESTAMP_ORDER => &INT96_TIMESTAMP_ORDER,
            ColumnOrder::UNDEFINED | ColumnOrder::UNKNOWN => {
                bail!("Parquet writer returned an undefined column order")
            }
        });
    }
    let mut expected = list_header(orders.len());
    expected.extend_from_slice(&entries);
    expected.push(STRUCT_STOP);
    if !footer.ends_with(&expected) {
        bail!("unexpected Parquet footer layout: the column orders are not its last field");
    }

    let entries_start = footer.len() - 1 - entries.len();
    for (index, order) in orders.iter().enumerate() {
        if *order == ColumnOrder::IEEE_754_TOTAL_ORDER {
            footer[entries_start + index * 3] = TYPE_ORDER[0];
        }
    }
    Ok(())
}

/// Compact-protocol list header for `len` structs.
fn list_header(len: usize) -> Vec<u8> {
    if len < 15 {
        return vec![((len as u8) << 4) | COMPACT_STRUCT];
    }
    let mut header = vec![0xF0 | COMPACT_STRUCT];
    let mut remaining = len as u64;
    loop {
        let byte = (remaining & 0x7F) as u8;
        remaining >>= 7;
        if remaining == 0 {
            header.push(byte);
            return header;
        }
        header.push(byte | 0x80);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, AsArray, Float32Array, Float64Array, UInt64Array};
    use arrow::datatypes::{Float32Type, Float64Type};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;
    use std::sync::Arc;

    fn write_file(path: &Path, columns: Vec<(String, ArrayRef)>) -> ParquetMetaData {
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap()
    }

    fn read_orders(path: &Path) -> Vec<ColumnOrder> {
        let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        reader
            .metadata()
            .file_metadata()
            .column_orders()
            .unwrap()
            .clone()
    }

    #[test]
    fn float_columns_get_type_order_and_others_are_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("floats.parquet");
        let metadata = write_file(
            &path,
            vec![
                (
                    "ticks".into(),
                    Arc::new(UInt64Array::from(vec![1_u64])) as ArrayRef,
                ),
                ("gain".into(), Arc::new(Float32Array::from(vec![0.5_f32]))),
                ("value".into(), Arc::new(Float64Array::from(vec![1.25_f64]))),
            ],
        );
        let before = read_orders(&path);
        assert_eq!(before[1], ColumnOrder::IEEE_754_TOTAL_ORDER);
        let len_before = std::fs::metadata(&path).unwrap().len();

        write_type_order_for_floats(&path, &metadata).unwrap();

        let after = read_orders(&path);
        assert_eq!(after[0], before[0]);
        assert!(matches!(after[1], ColumnOrder::TYPE_DEFINED_ORDER(_)));
        assert!(matches!(after[2], ColumnOrder::TYPE_DEFINED_ORDER(_)));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before);

        // The parquet crate's own reader still reads the data.
        let batches = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let gain = batches[0].column(1).as_primitive::<Float32Type>();
        let value = batches[0].column(2).as_primitive::<Float64Type>();
        assert_eq!((gain.value(0), value.value(0)), (0.5, 1.25));
    }

    #[test]
    fn rewrites_lists_of_fifteen_or_more_columns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wide.parquet");
        let columns = (0..20)
            .map(|i| {
                let array: ArrayRef = if i % 2 == 0 {
                    Arc::new(Float64Array::from(vec![i as f64]))
                } else {
                    Arc::new(UInt64Array::from(vec![i as u64]))
                };
                (format!("c{i}"), array)
            })
            .collect();
        let metadata = write_file(&path, columns);

        write_type_order_for_floats(&path, &metadata).unwrap();

        assert!(
            !read_orders(&path).contains(&ColumnOrder::IEEE_754_TOTAL_ORDER),
            "an IEEE 754 total order survived"
        );
    }

    #[test]
    fn leaves_an_unexpected_footer_alone() {
        let mut footer = vec![0x15, 0x02, 0x00];
        let orders = [ColumnOrder::IEEE_754_TOTAL_ORDER];
        assert!(rewrite_footer(&mut footer, &orders).is_err());
        assert_eq!(footer, [0x15, 0x02, 0x00]);
    }
}
