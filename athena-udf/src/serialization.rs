use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::writer::{
    write_message, DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use lambda_runtime::Error;
use std::sync::Arc;

/// Encodes a schema and its record batches into two Arrow IPC streams.
///
/// The schema stream holds the schema message. The records stream holds the
/// dictionary messages and the record batch messages. One dictionary tracker is
/// shared by both streams, so the dictionary ids in the records match the ids in
/// the schema. A tracker per stream would produce unmatched ids.
///
/// # Arguments
///
/// * `schema` - The schema the batches were built for
/// * `batches` - The record batches to encode
///
/// # Errors
///
/// Returns an error if the schema or any batch cannot be encoded.
pub(crate) fn encode_stream(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<(Vec<u8>, Vec<u8>), ArrowError> {
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut context = IpcWriteContext::default();

    let mut schema_buffer = Vec::new();
    let encoded_schema = generator.schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut dictionary_tracker,
        &options,
    );
    write_message(&mut schema_buffer, encoded_schema, &options)?;

    let mut records_buffer = Vec::new();
    for batch in batches {
        if batch.num_columns() != schema.fields().len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Record batch has {} columns, but the schema expects {}",
                batch.num_columns(),
                schema.fields().len()
            )));
        }
        for (index, (field, column)) in schema.fields().iter().zip(batch.columns()).enumerate() {
            if column.data_type() != field.data_type() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Record batch column {} (\"{}\") has type {}, but the schema expects {}",
                    index,
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
        }
        let (encoded_dictionaries, encoded_batch) =
            generator.encode(batch, &mut dictionary_tracker, &options, &mut context)?;

        for dictionary in encoded_dictionaries {
            write_message(&mut records_buffer, dictionary, &options)?;
        }

        write_message(&mut records_buffer, encoded_batch, &options)?;
    }

    Ok((schema_buffer, records_buffer))
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
    Ok(encode_stream(schema, &[])?.0)
}

/// Serializes Arrow record batches to IPC format.
///
/// Converts one or more Arrow record batches into a byte buffer using the Arrow IPC
/// streaming format. Each batch is encoded with its dictionaries (if any) and written
/// sequentially to the output buffer.
///
/// The batches must be built for the schema that is sent alongside them, because
/// dictionary ids are assigned per stream. [`crate::response::AthenaUDFResponse::from_batches`]
/// encodes both streams together and does not have this constraint.
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
    match batches.first() {
        Some(first) => Ok(encode_stream(&first.schema(), batches)?.1),
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::ipc::reader::StreamReader;
    use arrow::record_batch::RecordBatch;
    use std::io::Cursor;

    fn read_stream(mut stream: Vec<u8>) -> Vec<RecordBatch> {
        let reader = StreamReader::try_new(Cursor::new(&mut stream), None).unwrap();
        reader.collect::<Result<_, _>>().unwrap()
    }

    #[test]
    fn test_schema_with_padding_roundtrips_with_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .unwrap();

        let mut stream = serialize_schema(&schema).unwrap();
        stream.extend(serialize_batches(&[batch]).unwrap());

        let batches = read_stream(stream);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert!(batches[0].column(0).is_null(1));
    }

    #[test]
    fn test_serialize_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Utf8, true),
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
            DataType::Int32,
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
            DataType::Int32,
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

    #[test]
    fn test_dictionary_batches_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let dictionary: DictionaryArray<Int32Type> =
            vec![Some("a"), None, Some("b")].into_iter().collect();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(dictionary)]).unwrap();

        let (schema_buffer, records_buffer) = encode_stream(&schema, &[batch]).unwrap();
        let mut stream = schema_buffer;
        stream.extend(records_buffer);

        let batches = read_stream(stream);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].schema(), schema);
    }

    #[test]
    fn test_encode_stream_rejects_mismatched_batch_schema() {
        let declared = Schema::new(vec![Field::new("out", DataType::Float64, true)]);
        let batch_schema = Arc::new(Schema::new(vec![Field::new("out", DataType::Int32, true)]));
        let batch =
            RecordBatch::try_new(batch_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();

        let error = encode_stream(&declared, &[batch]).unwrap_err();
        assert!(error
            .to_string()
            .contains("has type Int32, but the schema expects Float64"));
    }
}
