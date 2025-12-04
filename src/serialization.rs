use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use lambda_runtime::Error;
use std::sync::Arc;

/// Writes an IPC message to a buffer following the Apache Arrow IPC format specification.
///
/// The message format consists of:
/// 1. Continuation marker (0xFFFFFFFF)
/// 2. Metadata size (4 bytes, little-endian)
/// 3. Metadata flatbuffer (padded to 8-byte alignment)
/// 4. Message body (arrow data)
///
/// # Arguments
///
/// * `buffer` - The output buffer to write the message to
/// * `encoded` - The encoded IPC data containing the message and arrow data
///
/// # References
///
/// See [Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc)
fn write_ipc_message(buffer: &mut Vec<u8>, encoded: &arrow::ipc::writer::EncodedData) {
    //https://arrow.apache.org/docs/format/Columnar.html#format-ipc

    // Continuation Marker
    buffer.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);

    let metadata_size = encoded.ipc_message.len() as i32;
    buffer.extend_from_slice(&metadata_size.to_le_bytes());

    // metadata flatbuffer
    buffer.extend_from_slice(&encoded.ipc_message);

    let padding = (8 - (encoded.ipc_message.len() % 8)) % 8;
    buffer.extend_from_slice(&vec![0u8; padding]);

    // message body
    buffer.extend_from_slice(&encoded.arrow_data);
}

/// Serializes an Arrow schema to IPC format.
///
/// Converts an Arrow schema into a byte buffer using the Arrow IPC streaming format.
/// The result can be transmitted and deserialized by Arrow-compatible systems.
///
/// # Arguments
///
/// * `schema` - The Arrow schema to serialize
///
/// # Returns
///
/// A byte vector containing the serialized schema in Arrow IPC format.
///
/// # Errors
///
/// Returns an error if the schema cannot be encoded.
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{Field, Schema};
/// # use athena_udf::serialization::serialize_schema;
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("col1", arrow::datatypes::DataType::Int32, false),
///     Field::new("col2", arrow::datatypes::DataType::Utf8, true),
/// ]));
///
/// let result = serialize_schema(&schema);
/// assert!(result.is_ok());
///
/// let buffer = result.unwrap();
/// assert!(!buffer.is_empty());
/// assert!(buffer.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]));
/// ```
pub fn serialize_schema(schema: &Arc<Schema>) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    let options = IpcWriteOptions::default();
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    let encoded_schema =
        data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut dictionary_tracker, &options);

    write_ipc_message(&mut buffer, &encoded_schema);
    Ok(buffer)
}
///
/// Serializes Arrow record batches to IPC format.
///
/// Converts one or more Arrow record batches into a byte buffer using the Arrow IPC
/// streaming format. Each batch is encoded with its dictionaries (if any) and written
/// sequentially to the output buffer.
///
/// # Arguments
///
/// * `batches` - A slice of record batches to serialize
///
/// # Returns
///
/// A byte vector containing the serialized batches in Arrow IPC format.
/// Returns an empty vector if the input slice is empty.
///
/// # Errors
///
/// Returns an error if any batch cannot be encoded.
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::array::Int32Array;
/// # use arrow::datatypes::{Field, Schema};
/// # use arrow::record_batch::RecordBatch;
/// # use athena_udf::serialization::serialize_batches;
/// // Empty batches
/// let result = serialize_batches(&[]);
/// assert!(result.is_ok());
/// assert_eq!(result.unwrap(), Vec::<u8>::new());
///
/// // Single batch
/// let schema = Arc::new(Schema::new(vec![Field::new(
///     "col1",
///     arrow::datatypes::DataType::Int32,
///     false,
/// )]));
///
/// let array = Int32Array::from(vec![1, 2, 3]);
/// let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
///
/// let result = serialize_batches(&[batch]);
/// assert!(result.is_ok());
///
/// let buffer = result.unwrap();
/// assert!(!buffer.is_empty());
/// assert!(buffer.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]));
/// ```
pub fn serialize_batches(batches: &[RecordBatch]) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();

    if !batches.is_empty() {
        let options = IpcWriteOptions::default();
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let mut compression_context = CompressionContext::default();

        for batch in batches {
            let (encoded_dictionaries, encoded_batch) = data_gen.encode(
                batch,
                &mut dictionary_tracker,
                &options,
                &mut compression_context,
            )?;

            for dict in encoded_dictionaries {
                write_ipc_message(&mut buffer, &dict);
            }

            write_ipc_message(&mut buffer, &encoded_batch);
        }
    }

    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_serialize_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", arrow::datatypes::DataType::Int32, false),
            Field::new("col2", arrow::datatypes::DataType::Utf8, true),
        ]));

        let result = serialize_schema(&schema);
        assert!(result.is_ok());

        let buffer = result.unwrap();
        assert!(!buffer.is_empty());
        assert!(buffer.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]));
    }

    #[test]
    fn test_serialize_empty_batches() {
        let result = serialize_batches(&[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_serialize_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            arrow::datatypes::DataType::Int32,
            false,
        )]));

        let array = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let result = serialize_batches(&[batch]);
        assert!(result.is_ok());

        let buffer = result.unwrap();
        assert!(!buffer.is_empty());
        assert!(buffer.starts_with(&[0xFF, 0xFF, 0xFF, 0xFF]));
    }

    #[test]
    fn test_serialize_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            arrow::datatypes::DataType::Int32,
            false,
        )]));

        let array1 = Int32Array::from(vec![1, 2]);
        let array2 = Int32Array::from(vec![3, 4]);
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(array1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(array2)]).unwrap();

        let result = serialize_batches(&[batch1, batch2]);
        assert!(result.is_ok());

        let buffer = result.unwrap();
        assert!(!buffer.is_empty());
    }
}
