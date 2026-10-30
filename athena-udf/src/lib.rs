//! Build AWS Athena user defined functions (UDF) on AWS Lambda with Apache Arrow.
//!
//! Athena calls the Lambda function with a JSON event. The event is either a
//! `PingRequest`, which checks that the function is reachable, or a
//! `UserDefinedFunctionRequest`, which carries the input rows and the declared
//! output type as base64-encoded Arrow IPC data.
//!
//! The crate handles the protocol:
//!
//! 1. [`handle_athena_request`] parses the event and reads the Arrow batches.
//! 2. [`UDFProcessor`] converts each row to Rust values, calls your function, and
//!    builds an Arrow array for the declared output type.
//! 3. The response is encoded back to Arrow IPC and base64.
//!
//! # Quick start
//!
//! ```
//! use athena_udf::*;
//! use lambda_runtime::{service_fn, run};
//! use serde_json::Value;
//!
//! pub fn string_reverse(value: String) -> String {
//!     value.chars().rev().collect()
//! }
//!
//! athena_udf_handler! {
//!     "string_reverse" => string_reverse: (String) -> String,
//! }
//!
//! # fn main() {}
//! # async fn run_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
//! #     handle_athena_request(event, |batch, method_name, output_field| {
//! #         match method_name {
//! #             "string_reverse" => UDFProcessor::new(batch)
//! #                 .process_unary::<String, String, _>(output_field, string_reverse),
//! #             _ => Err(format!("Unknown function: {}", method_name).into()),
//! #         }
//! #     }).await
//! # }
//! ```
//!
//! # Supported types
//!
//! | Athena SQL type | Rust type |
//! |-----------------|-----------|
//! | `VARCHAR`       | `String`    |
//! | `BIGINT`        | `i64`       |
//! | `INTEGER`       | `i32`       |
//! | `DOUBLE`        | `f64`       |
//! | `BOOLEAN`       | `bool`      |
//! | `VARBINARY`     | `Vec<u8>`   |
//! | `ROW(...)`      | [`StructValue`] |
//!
//! Wrap a type in `Option<T>` to receive nulls instead of skipping the row, and
//! to return null explicitly.

pub mod arrow_conversions;
pub mod process_macro;
pub mod register_macro;
pub mod request;
pub mod response;
pub mod serde_base64;
pub mod serialization;
pub mod struct_value;

use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
pub use arrow_conversions::{FromArrow, ToArrow};
pub use lambda_runtime::Error;
pub use process_macro::UDFProcessor;
pub use request::{AthenaUDFRequest, Identity, InputRecords, OutputSchemaWrapper, PingRequest};
pub use response::{AthenaResponse, AthenaUDFResponse, OutputRecords, PingResponse};
pub use struct_value::{ScalarValue, StructValue};

pub use lambda_runtime::{run, service_fn, LambdaEvent};
pub use serde::{Deserialize, Serialize};
pub use serde_json::Value;

/// Wraps an Athena response in the HTTP envelope when the request came through
/// API Gateway or a Lambda function URL.
pub fn wrap_response(response: AthenaResponse, is_http: bool) -> Result<Value, Error> {
    response.wrap_response(is_http)
}

/// A handler function type for processing UDF requests.
///
/// Takes the input batch, method name, and the declared output field,
/// and returns a processed RecordBatch.
pub type UDFHandler = fn(&RecordBatch, &str, &Field) -> Result<RecordBatch, Error>;

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
///     handle_athena_request(event, |input_batch, method_name, output_field| {
///         match method_name {
///             "string_reverse" => UDFProcessor::new(input_batch)
///                 .process_unary::<String, String, _>(output_field, string_reverse),
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
    F: Fn(&RecordBatch, &str, &Field) -> Result<RecordBatch, Error>,
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
