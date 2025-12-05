use crate::request::PingRequest;
use crate::serialization::{serialize_batches, serialize_schema};
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use lambda_runtime::Error;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

/// Represents the response types that can be returned from AWS Athena Lambda handlers.
///
/// This enum encapsulates both ping responses (for health checks) and UDF responses
/// (containing actual computation results).
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum AthenaResponse {
    PingResponse(PingResponse),
    UserDefinedFunctionResponse(AthenaUDFResponse),
}

/// Response to an Athena ping request, confirming the Lambda function is operational.
///
/// Contains metadata about the UDF handler's capabilities and configuration.
///
/// # Examples
///
/// ```
/// # use athena_udf::request::PingRequest;
/// # use athena_udf::request::Identity;
/// let ping_request = PingRequest {
///     request_type: "PingRequest".to_string(),
///     identity: Identity {
///         id: Some("test-id".to_string()),
///         principal: None,
///         account: Some("123456".to_string()),
///         arn: None,
///     },
///     catalog_name: Some("test_catalog".to_string()),
///     query_id: Some("query-123".to_string()),
/// };
///
/// let response = ping_request.handle();
/// ```
#[derive(Debug, Serialize)]
pub struct PingResponse {
    #[serde(rename = "@type")]
    response_type: String,
    #[serde(rename = "catalogName")]
    catalog_name: Option<String>,
    #[serde(rename = "queryId")]
    query_id: Option<String>,
    #[serde(rename = "sourceType")]
    source_type: String,
    capabilities: u64,
    #[serde(rename = "serdeVersion", skip_serializing_if = "Option::is_none")]
    serde_version: Option<u64>,
}

/// Response containing the results of a User Defined Function execution.
///
/// The response includes the processed data as Apache Arrow record batches,
/// serialized in IPC format and base64-encoded for transmission.
#[derive(Debug, Serialize)]
pub struct AthenaUDFResponse {
    #[serde(rename = "@type")]
    pub response_type: String,
    #[serde(rename = "methodName")]
    pub method_name: String,
    pub records: OutputRecords,
}

/// Contains the output records from a UDF execution in Apache Arrow IPC format.
///
/// Both schema and records are base64-encoded Arrow IPC streams that Athena
/// will decode to retrieve the results.
#[derive(Debug, Serialize)]
pub struct OutputRecords {
    #[serde(rename = "aId")]
    pub a_id: String,
    #[serde(with = "crate::serde_base64")]
    pub schema: Vec<u8>,
    #[serde(with = "crate::serde_base64")]
    pub records: Vec<u8>,
}

impl PingRequest {
    /// Handles a ping request by creating an appropriate ping response.
    ///
    /// # Examples
    ///
    /// ```
    /// # use athena_udf::request::PingRequest;
    /// # use athena_udf::response::AthenaResponse;
    /// # use athena_udf::request::Identity;
    /// let ping_request = PingRequest {
    ///     request_type: "PingRequest".to_string(),
    ///     identity: Identity {
    ///         id: Some("test-id".to_string()),
    ///         principal: None,
    ///         account: Some("123456".to_string()),
    ///         arn: None,
    ///     },
    ///     catalog_name: Some("test_catalog".to_string()),
    ///     query_id: Some("query-123".to_string()),
    /// };
    ///
    /// let response = ping_request.handle();
    ///
    /// match response {
    ///     AthenaResponse::PingResponse(ping_resp) => {
    ///         // successful ping response
    ///     }
    ///     _ => panic!("Expected PingResponse"),
    /// }
    /// ```
    pub fn handle(self) -> AthenaResponse {
        AthenaResponse::PingResponse(PingResponse {
            response_type: "PingResponse".to_string(),
            catalog_name: self.catalog_name,
            query_id: self.query_id,
            source_type: "athena_udf_rust".to_string(),
            capabilities: 23,
            serde_version: Some(5),
        })
    }
}

impl AthenaUDFResponse {
    /// Creates a UDF response from Arrow record batches.
    ///
    /// This method serializes the schema and batches into Arrow IPC format,
    /// which is then base64-encoded for transmission back to Athena.
    ///
    /// # Arguments
    ///
    /// * `method_name` - The name of the UDF method that was invoked
    /// * `a_id` - The request identifier from the input records
    /// * `schema` - The output schema defining the structure of the results
    /// * `batches` - The computed result batches
    ///
    /// # Returns
    ///
    /// An `AthenaUDFResponse` ready to be serialized and returned to Athena.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema or batches cannot be serialized to Arrow IPC format.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::array::Int32Array;
    /// # use arrow::datatypes::{Field, Schema};
    /// # use arrow::record_batch::RecordBatch;
    /// # use athena_udf::response::AthenaUDFResponse;
    /// let schema = Arc::new(Schema::new(vec![Field::new(
    ///     "result",
    ///     arrow::datatypes::DataType::Int32,
    ///     false,
    /// )]));
    ///
    /// let array = Int32Array::from(vec![1, 2, 3]);
    /// let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    ///
    /// let response = AthenaUDFResponse::from_batches(
    ///     "test_function".to_string(),
    ///     "test-id-123".to_string(),
    ///     &schema,
    ///     vec![batch],
    /// );
    ///
    /// assert!(response.is_ok());
    /// let response = response.unwrap();
    /// assert_eq!(response.method_name, "test_function");
    /// assert_eq!(response.records.a_id, "test-id-123");
    /// ```
    pub fn from_batches(
        method_name: String,
        a_id: String,
        schema: &Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self, Error> {
        let schema_buffer = serialize_schema(schema)?;
        let records_buffer = serialize_batches(&batches)?;

        Ok(AthenaUDFResponse {
            response_type: "UserDefinedFunctionResponse".to_string(),
            method_name,
            records: OutputRecords {
                a_id,
                schema: schema_buffer,
                records: records_buffer,
            },
        })
    }
}

impl AthenaResponse {
    /// Parses an incoming request payload, handling both direct and HTTP-wrapped formats.
    ///
    /// AWS Lambda can invoke functions directly or via HTTP (API Gateway/Function URLs).
    /// This method detects the format and extracts the actual request payload.
    ///
    /// # Arguments
    ///
    /// * `payload` - The raw JSON payload from Lambda
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The parsed request payload
    /// - A boolean indicating if the request was HTTP-wrapped (true) or direct (false)
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be parsed as valid JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// # use athena_udf::request::{PingRequest, Identity};
    /// let ping_request = PingRequest {
    ///     request_type: "PingRequest".to_string(),
    ///     identity: Identity {
    ///         id: None,
    ///         principal: None,
    ///         account: None,
    ///         arn: None,
    ///     },
    ///     catalog_name: None,
    ///     query_id: None,
    /// };
    ///
    /// let response = ping_request.handle();
    ///
    /// // Direct invocation
    /// let wrapped = response.wrap_response(false);
    /// assert!(wrapped.is_ok());
    /// let wrapped = wrapped.unwrap();
    /// assert!(wrapped.get("statusCode").is_none());
    ///
    /// // HTTP invocation - create a new ping_request
    /// let ping_request2 = PingRequest {
    ///     request_type: "PingRequest".to_string(),
    ///     identity: Identity {
    ///         id: None,
    ///         principal: None,
    ///         account: None,
    ///         arn: None,
    ///     },
    ///     catalog_name: None,
    ///     query_id: None,
    /// };
    /// let response = ping_request2.handle();
    /// let wrapped = response.wrap_response(true);
    /// assert!(wrapped.is_ok());
    /// let wrapped = wrapped.unwrap();
    /// assert_eq!(wrapped.get("statusCode").unwrap(), 200);
    /// assert_eq!(wrapped.get("isBase64Encoded").unwrap(), false);
    /// ```
    pub fn parse_request(payload: Value) -> Result<(Value, bool), Error> {
        if let Some(body) = payload.get("body").and_then(|v| v.as_str()) {
            Ok((serde_json::from_str(body)?, true))
        } else {
            Ok((payload, false))
        }
    }

    /// Wraps the response in the appropriate format based on invocation type.
    ///
    /// For HTTP invocations (API Gateway/Function URLs), wraps the response in an
    /// HTTP response structure with status code and headers. For direct invocations,
    /// returns the response as-is.
    ///
    /// # Arguments
    ///
    /// * `is_http` - Whether the original request was HTTP-wrapped
    ///
    /// # Returns
    ///
    /// A JSON value ready to be returned from the Lambda handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the response cannot be serialized to JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// # use athena_udf::request::{PingRequest, Identity};
    /// # use athena_udf::response::AthenaUDFResponse;
    /// let ping_request = PingRequest {
    ///     request_type: "PingRequest".to_string(),
    ///     identity: Identity {
    ///         id: None,
    ///         principal: None,
    ///         account: None,
    ///         arn: None,
    ///     },
    ///     catalog_name: None,
    ///     query_id: None,
    /// };
    ///
    /// let response = ping_request.handle();
    ///
    /// let wrapped = response.wrap_response(false);
    /// assert!(wrapped.is_ok());
    /// let wrapped = wrapped.unwrap();
    /// assert!(wrapped.get("statusCode").is_none());
    ///
    /// ```
    pub fn wrap_response(self, is_http: bool) -> Result<Value, Error> {
        if is_http {
            Ok(serde_json::json!({
                "statusCode": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": serde_json::to_string(&self)?,
                "cookies": [],
                "isBase64Encoded": false
            }))
        } else {
            Ok(serde_json::to_value(self)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::Identity;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_ping_request_handle() {
        let ping_request = PingRequest {
            request_type: "PingRequest".to_string(),
            identity: Identity {
                id: Some("test-id".to_string()),
                principal: None,
                account: Some("123456".to_string()),
                arn: None,
            },
            catalog_name: Some("test_catalog".to_string()),
            query_id: Some("query-123".to_string()),
        };

        let response = ping_request.handle();

        match response {
            AthenaResponse::PingResponse(ping_resp) => {
                assert_eq!(ping_resp.source_type, "athena_udf_rust");
                assert_eq!(ping_resp.capabilities, 23);
                assert_eq!(ping_resp.serde_version, Some(5));
                assert_eq!(ping_resp.catalog_name, Some("test_catalog".to_string()));
                assert_eq!(ping_resp.query_id, Some("query-123".to_string()));
            }
            _ => panic!("Expected PingResponse"),
        }
    }

    #[test]
    fn test_udf_response_from_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "result",
            arrow::datatypes::DataType::Int32,
            false,
        )]));

        let array = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();

        let response = AthenaUDFResponse::from_batches(
            "test_function".to_string(),
            "test-id-123".to_string(),
            &schema,
            vec![batch],
        );

        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.method_name, "test_function");
        assert_eq!(response.records.a_id, "test-id-123");
        assert!(!response.records.schema.is_empty());
        assert!(!response.records.records.is_empty());
    }

    #[test]
    fn test_parse_request_direct() {
        let payload = serde_json::json!({
            "@type": "PingRequest",
            "identity": {},
        });

        let result = AthenaResponse::parse_request(payload);
        assert!(result.is_ok());

        let (parsed, is_http) = result.unwrap();
        assert_eq!(is_http, false);
        assert_eq!(parsed.get("@type").unwrap(), "PingRequest");
    }

    #[test]
    fn test_parse_request_http_wrapped() {
        let inner = serde_json::json!({
            "@type": "PingRequest",
            "identity": {},
        });

        let payload = serde_json::json!({
            "body": serde_json::to_string(&inner).unwrap(),
        });

        let result = AthenaResponse::parse_request(payload);
        assert!(result.is_ok());

        let (parsed, is_http) = result.unwrap();
        assert_eq!(is_http, true);
        assert_eq!(parsed.get("@type").unwrap(), "PingRequest");
    }

    #[test]
    fn test_wrap_response_direct() {
        let ping_request = PingRequest {
            request_type: "PingRequest".to_string(),
            identity: Identity {
                id: None,
                principal: None,
                account: None,
                arn: None,
            },
            catalog_name: None,
            query_id: None,
        };

        let response = ping_request.handle();
        let wrapped = response.wrap_response(false);

        assert!(wrapped.is_ok());
        let wrapped = wrapped.unwrap();
        assert!(wrapped.get("statusCode").is_none());
    }

    #[test]
    fn test_wrap_response_http() {
        let ping_request = PingRequest {
            request_type: "PingRequest".to_string(),
            identity: Identity {
                id: None,
                principal: None,
                account: None,
                arn: None,
            },
            catalog_name: None,
            query_id: None,
        };

        let response = ping_request.handle();
        let wrapped = response.wrap_response(true);

        assert!(wrapped.is_ok());
        let wrapped = wrapped.unwrap();
        assert_eq!(wrapped.get("statusCode").unwrap(), 200);
        assert!(wrapped.get("body").is_some());
        assert_eq!(wrapped.get("isBase64Encoded").unwrap(), false);
    }
}
