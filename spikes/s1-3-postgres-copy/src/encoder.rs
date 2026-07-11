//! Arrow `RecordBatch` → PostgreSQL `COPY ... FROM STDIN (FORMAT binary)`
//! encoder for the v1 type matrix subset: Int32, Int64, Float64, Utf8,
//! Boolean, and Timestamp(µs, UTC) → timestamptz.
//!
//! Binary COPY layout: 11-byte signature + 4-byte flags + 4-byte extension
//! length header, then per tuple a big-endian i16 field count followed by,
//! per field, a big-endian i32 byte length (-1 for NULL) and the payload.
//! The stream ends with a single i16 of -1.

use anyhow::{Result, bail};
use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::{BufMut, BytesMut};

/// Microseconds between the Unix epoch and the PostgreSQL epoch (2000-01-01).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

pub fn copy_header() -> BytesMut {
    let mut buf = BytesMut::with_capacity(19);
    buf.put_slice(b"PGCOPY\n\xff\r\n\0");
    buf.put_i32(0); // flags
    buf.put_i32(0); // header extension length
    buf
}

pub fn copy_trailer() -> BytesMut {
    let mut buf = BytesMut::with_capacity(2);
    buf.put_i16(-1);
    buf
}

/// Append every row of `batch` to `buf` in binary COPY tuple format.
pub fn encode_batch(batch: &RecordBatch, buf: &mut BytesMut) -> Result<()> {
    let columns = batch.columns();
    let field_count = i16::try_from(columns.len())?;

    for row in 0..batch.num_rows() {
        buf.put_i16(field_count);
        for column in columns {
            if column.is_null(row) {
                buf.put_i32(-1);
                continue;
            }
            match column.data_type() {
                DataType::Int32 => {
                    let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    buf.put_i32(4);
                    buf.put_i32(array.value(row));
                }
                DataType::Int64 => {
                    let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    buf.put_i32(8);
                    buf.put_i64(array.value(row));
                }
                DataType::Float64 => {
                    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    buf.put_i32(8);
                    buf.put_f64(array.value(row));
                }
                DataType::Boolean => {
                    let array = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                    buf.put_i32(1);
                    buf.put_u8(array.value(row) as u8);
                }
                DataType::Utf8 => {
                    let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    let value = array.value(row).as_bytes();
                    buf.put_i32(i32::try_from(value.len())?);
                    buf.put_slice(value);
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    buf.put_i32(8);
                    buf.put_i64(array.value(row) - PG_EPOCH_OFFSET_US);
                }
                other => bail!("type not in the spike matrix: {other}"),
            }
        }
    }
    Ok(())
}
