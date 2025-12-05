pub mod arrow_conversions;
pub mod process_macro;
pub mod register_macro;
pub mod request;
pub mod response;
pub mod serde_base64;
pub mod serialization;

use arrow::record_batch::RecordBatch;
pub use arrow_conversions::{FromArrow, ToArrow};
pub use process_macro::UDFProcessor;
pub use request::{AthenaUDFRequest, Identity, InputRecords, OutputSchemaWrapper, PingRequest};
pub use response::{AthenaResponse, AthenaUDFResponse, OutputRecords, PingResponse};

pub use lambda_runtime::{run, service_fn, LambdaEvent};
pub use serde::{Deserialize, Serialize};
pub use serde_json::Value;

// Re-export for backwards compatibility
use lambda_runtime::Error;

pub fn wrap_response(response: AthenaResponse, is_http: bool) -> Result<Value, Error> {
    response.wrap_response(is_http)
}

/// A handler function type for processing UDF requests.
///
/// Takes the input batch, method name, and output column name,
/// and returns a processed RecordBatch.
pub type UDFHandler = fn(&RecordBatch, &str, &str) -> Result<RecordBatch, Error>;

/// Main entry point for Athena UDF Lambda handlers.
///
/// Automatically handles both PingRequest and UserDefinedFunctionRequest,
/// routing UDF calls to the provided handler function.
///
/// # Examples
///
/// ```no_run
/// use athena_udf::*;
/// use lambda_runtime::{service_fn, run, Error};
///
/// fn string_reverse(s: String) -> String {
///     s.chars().rev().collect()
/// }
///
/// async fn function_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
///     handle_athena_request(event, |input_batch, method_name, output_col_name| {
///         match method_name {
///             "string_reverse" => UDFProcessor::new(input_batch)
///                 .process_unary::<String, String, _>(output_col_name, string_reverse),
///             _ => Err(format!("Unknown function: {}", method_name).into()),
///         }
///     }).await
/// }
/// ```
pub async fn handle_athena_request<F>(
    event: LambdaEvent<Value>,
    udf_handler: F,
) -> Result<Value, Error>
where
    F: Fn(&RecordBatch, &str, &str) -> Result<RecordBatch, Error>,
{
    let (actual_payload, is_http) = AthenaResponse::parse_request(event.payload)?;

    let request_type = actual_payload
        .get("@type")
        .and_then(|v| v.as_str())
        .ok_or("Missing @type field")?;

    let response = match request_type {
        "PingRequest" => {
            let ping_req: PingRequest = serde_json::from_value(actual_payload)?;
            ping_req.handle()
        }
        "UserDefinedFunctionRequest" => {
            let udf_req: AthenaUDFRequest = serde_json::from_value(actual_payload)?;
            udf_req.process_with(&udf_handler)?
        }
        _ => return Err(format!("Unknown request type: {}", request_type).into()),
    };

    response.wrap_response(is_http)
}
