use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamReader;
use lambda_runtime::Error;
use serde::Deserialize;
use std::io::Cursor;
use std::sync::Arc;

/// Represents a ping request from AWS Athena to verify Lambda function connectivity.
///
/// This is typically the first request sent to validate that the UDF handler is
/// properly configured and responsive.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PingRequest {
    #[serde(rename = "@type")]
    pub request_type: String,
    pub identity: Identity,
    pub catalog_name: Option<String>,
    pub query_id: Option<String>,
}

/// Represents a User Defined Function (UDF) request from AWS Athena.
///
/// This structure contains all the information needed to process a UDF invocation,
/// including input data in Arrow IPC format, the output schema, and metadata about
/// the function being called.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AthenaUDFRequest {
    #[serde(rename = "@type")]
    pub request_type: String,
    pub identity: Identity,
    pub input_records: InputRecords,
    #[serde(rename = "outputSchema")]
    pub output_schema: OutputSchemaWrapper,
    pub method_name: String,
    pub function_type: String,
}

/// Contains identity information about the principal making the Athena request.
///
/// This includes AWS account details, ARN, and other identifying information
/// that can be used for authorization and auditing purposes.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub id: Option<String>,
    pub principal: Option<String>,
    pub account: Option<String>,
    pub arn: Option<String>,
}

/// Contains the input records for a UDF invocation in Apache Arrow IPC format.
///
/// Both the schema and records are base64-encoded Arrow IPC streams that need
/// to be decoded and combined to reconstruct the input data.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InputRecords {
    #[serde(rename = "aId")]
    pub a_id: String,
    #[serde(with = "crate::serde_base64")]
    pub schema: Vec<u8>,
    #[serde(with = "crate::serde_base64")]
    pub records: Vec<u8>,
}

/// Wrapper for the output schema specification in Apache Arrow IPC format.
///
/// The schema is base64-encoded and defines the structure of the expected
/// output from the UDF.
#[derive(Debug, Deserialize)]
pub struct OutputSchemaWrapper {
    #[serde(with = "crate::serde_base64")]
    pub schema: Vec<u8>,
}

impl AthenaUDFRequest {
    /// Reads and deserializes the input record batches from the request.
    ///
    /// This method combines the schema and records from the `input_records` field,
    /// which are stored as separate base64-encoded Arrow IPC streams, and deserializes
    /// them into Arrow `RecordBatch` objects.
    ///
    /// # Returns
    ///
    /// A vector of `RecordBatch` objects containing the input data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow IPC stream cannot be parsed
    /// - The schema and records are incompatible
    /// - Any batch fails to deserialize
    pub fn read_input_batches(&self) -> Result<Vec<RecordBatch>, Error> {
        let schema_bytes = &self.input_records.schema;
        let record_bytes = &self.input_records.records;

        let mut combined_data = Vec::with_capacity(schema_bytes.len() + record_bytes.len());
        combined_data.extend_from_slice(schema_bytes);
        combined_data.extend_from_slice(record_bytes);

        let cursor = Cursor::new(combined_data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| format!("Failed to create StreamReader: {}", e))?;

        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Error reading batch: {}", e).into())
    }

    /// Reads and deserializes the output schema from the request.
    ///
    /// The output schema defines the structure that the UDF's output must conform to.
    ///
    /// # Returns
    ///
    /// An `Arc<Schema>` containing the expected output schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be parsed from the Arrow IPC format.
    pub fn read_output_schema(&self) -> Result<Arc<Schema>, Error> {
        let cursor = Cursor::new(&self.output_schema.schema);
        let reader = StreamReader::try_new(cursor, None)?;
        Ok(reader.schema())
    }

    /// Processes the UDF request using the provided processor function.
    ///
    /// This is a convenience method that:
    /// 1. Reads the input batches
    /// 2. Reads the output schema
    /// 3. Applies the processor function to each input batch
    /// 4. Constructs an `AthenaResponse` from the results
    ///
    /// # Arguments
    ///
    /// * `processor` - A function that takes a `RecordBatch`, the method name, and the
    ///   declared output field, and returns a transformed `RecordBatch`
    ///
    /// # Returns
    ///
    /// An `AthenaResponse` containing the processed results.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request is not a scalar function request
    /// - Input batches cannot be read
    /// - The output schema is empty or cannot be read
    /// - The processor function returns an error for any batch
    /// - The response cannot be constructed from the output batches
    pub fn process_with<F>(self, mut processor: F) -> Result<crate::response::AthenaResponse, Error>
    where
        F: FnMut(&RecordBatch, &str, &Field) -> Result<RecordBatch, Error>,
    {
        if !self.function_type.eq_ignore_ascii_case("SCALAR") {
            return Err(format!(
                "Unsupported functionType: {}. Only SCALAR functions are supported",
                self.function_type
            )
            .into());
        }

        let input_batches = self.read_input_batches()?;
        let output_schema = self.read_output_schema()?;
        let output_field = output_schema
            .fields()
            .first()
            .ok_or("The output schema has no fields")?;

        let output_batches: Result<Vec<RecordBatch>, Error> = input_batches
            .iter()
            .map(|batch| processor(batch, &self.method_name, output_field))
            .collect();

        let response = crate::response::AthenaUDFResponse::from_batches(
            self.method_name.clone(),
            self.input_records.a_id.clone(),
            &output_schema,
            output_batches?,
        )?;

        Ok(crate::response::AthenaResponse::UserDefinedFunctionResponse(response))
    }
}
