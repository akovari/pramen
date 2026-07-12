//! Arrow `RecordBatch` → PostgreSQL binary `COPY` frame encoding.
//!
//! Wire layout (PostgreSQL documentation, "Binary Format"): an 11-byte
//! signature, 4-byte flags, and 4-byte extension-length header; then one
//! tuple per row — a big-endian `i16` field count followed by, per field, a
//! big-endian `i32` byte length (`-1` for NULL) and the raw payload; and a
//! single `i16` of `-1` as the trailer.
//!
//! Supported Arrow types (the v1 matrix subset validated in spike S1.3):
//! `Int32`, `Int64`, `Float64`, `Utf8`/`LargeUtf8`/`Utf8View`, `Boolean`,
//! and `Timestamp(Microsecond, _)` (loaded as `timestamptz`).

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, BytesMut};
use pramen_core::runtime::StageError;

/// Microseconds between the Unix epoch and the PostgreSQL epoch
/// (2000-01-01T00:00:00Z).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// The 19-byte stream header.
pub(super) fn copy_header() -> BytesMut {
    let mut buf = BytesMut::with_capacity(19);
    buf.put_slice(b"PGCOPY\n\xff\r\n\0");
    buf.put_i32(0); // flags
    buf.put_i32(0); // header extension length
    buf
}

/// The 2-byte stream trailer.
pub(super) fn copy_trailer() -> BytesMut {
    let mut buf = BytesMut::with_capacity(2);
    buf.put_i16(-1);
    buf
}

/// Downcast helper: the runner guarantees the array matches its declared
/// data type, so a failed downcast is a programming error surfaced as
/// `InvalidData` rather than a panic.
fn downcast<'a, T: 'static>(column: &'a dyn Array, context: &str) -> Result<&'a T, StageError> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| StageError::InvalidData(format!("array/type mismatch for {context}")))
}

fn put_text(buf: &mut BytesMut, value: &str) -> Result<(), StageError> {
    let bytes = value.as_bytes();
    let len = i32::try_from(bytes.len())
        .map_err(|_| StageError::InvalidData("string value exceeds 2 GiB".to_owned()))?;
    buf.put_i32(len);
    buf.put_slice(bytes);
    Ok(())
}

/// Append every row of `batch` to `buf` as binary `COPY` tuples.
///
/// # Errors
///
/// Returns [`StageError::InvalidData`] for Arrow types outside the v1
/// matrix or values that cannot be represented.
// `pub` (not `pub(super)`) so the Criterion benchmark harness can
// measure the encoder in isolation; it is not part of the stable API.
#[doc(hidden)]
pub fn encode_batch(batch: &RecordBatch, buf: &mut BytesMut) -> Result<(), StageError> {
    let columns = batch.columns();
    let field_count = i16::try_from(columns.len())
        .map_err(|_| StageError::InvalidData("more than 32767 columns".to_owned()))?;

    for row in 0..batch.num_rows() {
        buf.put_i16(field_count);
        for column in columns {
            if column.is_null(row) {
                buf.put_i32(-1);
                continue;
            }
            match column.data_type() {
                DataType::Int32 => {
                    let array: &Int32Array = downcast(column.as_ref(), "Int32")?;
                    buf.put_i32(4);
                    buf.put_i32(array.value(row));
                }
                DataType::Int64 => {
                    let array: &Int64Array = downcast(column.as_ref(), "Int64")?;
                    buf.put_i32(8);
                    buf.put_i64(array.value(row));
                }
                DataType::Float64 => {
                    let array: &Float64Array = downcast(column.as_ref(), "Float64")?;
                    buf.put_i32(8);
                    buf.put_f64(array.value(row));
                }
                DataType::Boolean => {
                    let array: &BooleanArray = downcast(column.as_ref(), "Boolean")?;
                    buf.put_i32(1);
                    buf.put_u8(u8::from(array.value(row)));
                }
                DataType::Utf8 => {
                    let array: &StringArray = downcast(column.as_ref(), "Utf8")?;
                    put_text(buf, array.value(row))?;
                }
                DataType::LargeUtf8 => {
                    let array: &LargeStringArray = downcast(column.as_ref(), "LargeUtf8")?;
                    put_text(buf, array.value(row))?;
                }
                DataType::Utf8View => {
                    let array: &StringViewArray = downcast(column.as_ref(), "Utf8View")?;
                    put_text(buf, array.value(row))?;
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let array: &TimestampMicrosecondArray = downcast(column.as_ref(), "Timestamp")?;
                    buf.put_i32(8);
                    buf.put_i64(array.value(row) - PG_EPOCH_OFFSET_US);
                }
                other => {
                    return Err(StageError::InvalidData(format!(
                        "Arrow type {other} is not in the v1 Postgres sink matrix"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Float64Array::from(vec![1.5])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn frame_structure_is_correct() {
        let mut buf = copy_header();
        encode_batch(&sample_batch(), &mut buf).unwrap();
        buf.extend_from_slice(&copy_trailer());

        assert_eq!(&buf[..11], b"PGCOPY\n\xff\r\n\0");
        // Tuple field count.
        assert_eq!(i16::from_be_bytes([buf[19], buf[20]]), 3);
        // id: length 8, value 7.
        assert_eq!(i32::from_be_bytes([buf[21], buf[22], buf[23], buf[24]]), 8);
        assert_eq!(i64::from_be_bytes(buf[25..33].try_into().unwrap()), 7);
        // score: length 8. note: NULL (-1). Trailer -1.
        assert_eq!(i32::from_be_bytes([buf[33], buf[34], buf[35], buf[36]]), 8);
        assert_eq!(i32::from_be_bytes([buf[45], buf[46], buf[47], buf[48]]), -1);
        assert_eq!(
            i16::from_be_bytes([buf[buf.len() - 2], buf[buf.len() - 1]]),
            -1
        );
    }

    #[test]
    fn unsupported_types_are_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let array = arrow::array::Decimal128Array::from(vec![1_i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let mut buf = BytesMut::new();
        let error = encode_batch(&batch, &mut buf).unwrap_err();
        assert!(error.to_string().contains("not in the v1"), "{error}");
    }
}
