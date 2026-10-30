//! End-to-end tests for the Athena request and response protocol.

use arrow::array::{Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use athena_udf::serialization::{serialize_batches, serialize_schema};
use athena_udf::*;
use base64::{engine::general_purpose, Engine as _};
use lambda_runtime::LambdaEvent;
use std::io::Cursor;
use std::sync::Arc;

fn b64(bytes: &[u8]) -> String {
    general_purpose::STANDARD.encode(bytes)
}

fn decode(bytes: &str) -> Vec<u8> {
    general_purpose::STANDARD.decode(bytes).unwrap()
}

fn request(
    input_schema: &Arc<Schema>,
    records: &[u8],
    output_schema: &Schema,
    method_name: &str,
    function_type: &str,
) -> serde_json::Value {
    serde_json::json!({
        "@type": "UserDefinedFunctionRequest",
        "identity": {"id": "test", "account": "123456"},
        "inputRecords": {
            "aId": "request-id",
            "schema": b64(&serialize_schema(input_schema).unwrap()),
            "records": b64(records),
        },
        "outputSchema": {"schema": b64(&serialize_schema(&Arc::new(output_schema.clone())).unwrap())},
        "methodName": method_name,
        "functionType": function_type,
    })
}

fn event(payload: serde_json::Value) -> LambdaEvent<serde_json::Value> {
    LambdaEvent::new(payload, Default::default())
}

fn read_response_batches(value: &serde_json::Value) -> Vec<RecordBatch> {
    let mut stream = decode(value["records"]["schema"].as_str().unwrap());
    stream.extend(decode(value["records"]["records"].as_str().unwrap()));
    let reader = StreamReader::try_new(Cursor::new(stream), None).unwrap();
    reader.collect::<Result<_, _>>().unwrap()
}

fn string_input(values: Vec<Option<&str>>) -> (Arc<Schema>, Vec<u8>) {
    let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
    (schema, serialize_batches(&[batch]).unwrap())
}

#[tokio::test]
async fn ping_request_returns_ping_response() {
    let payload = serde_json::json!({
        "@type": "PingRequest",
        "identity": {"id": "test"},
        "catalogName": "AwsDataCatalog",
        "queryId": "query-1",
    });

    let value = handle_athena_request(event(payload), |_, _, _| unreachable!())
        .await
        .unwrap();

    assert_eq!(value["@type"], "PingResponse");
    assert_eq!(value["catalogName"], "AwsDataCatalog");
    assert_eq!(value["queryId"], "query-1");
    assert_eq!(value["sourceType"], "athena_udf_rust");
    assert_eq!(value["capabilities"], 23);
    assert_eq!(value["serdeVersion"], 5);
}

#[tokio::test]
async fn udf_request_round_trip() {
    let (input_schema, records) = string_input(vec![Some("hello"), None, Some("world")]);
    let output_schema = Schema::new(vec![Field::new("result", DataType::Utf8, true)]);

    let value = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "string_reverse",
            "SCALAR",
        )),
        |batch, method_name, output_field| {
            assert_eq!(method_name, "string_reverse");
            assert_eq!(output_field.name(), "result");
            UDFProcessor::new(batch)
                .process_unary::<String, String, _>(output_field, |s| s.chars().rev().collect())
        },
    )
    .await
    .unwrap();

    assert_eq!(value["@type"], "UserDefinedFunctionResponse");
    assert_eq!(value["methodName"], "string_reverse");
    assert_eq!(value["records"]["aId"], "request-id");

    let batches = read_response_batches(&value);
    assert_eq!(batches.len(), 1);
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(result.value(0), "olleh");
    assert!(result.is_null(1));
    assert_eq!(result.value(2), "dlrow");
}

#[tokio::test]
async fn http_wrapped_request_gets_http_response() {
    let payload = serde_json::json!({
        "body": serde_json::json!({"@type": "PingRequest", "identity": {}}).to_string(),
    });

    let value = handle_athena_request(event(payload), |_, _, _| unreachable!())
        .await
        .unwrap();

    assert_eq!(value["statusCode"], 200);
    assert_eq!(value["isBase64Encoded"], false);
    let body: serde_json::Value = serde_json::from_str(value["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["@type"], "PingResponse");
}

#[tokio::test]
async fn struct_input_reaches_the_udf() {
    let struct_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("size", DataType::Int64, true),
    ]);
    let rows = StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("athena"), None])),
            Arc::new(Int64Array::from(vec![Some(7), Some(9)])),
        ],
        None,
    );

    let input_schema = Arc::new(Schema::new(vec![Field::new(
        "row",
        DataType::Struct(struct_fields),
        true,
    )]));
    let batch = RecordBatch::try_new(input_schema.clone(), vec![Arc::new(rows)]).unwrap();
    let output_schema = Schema::new(vec![Field::new("result", DataType::Utf8, true)]);

    let value = handle_athena_request(
        event(request(
            &input_schema,
            &serialize_batches(&[batch]).unwrap(),
            &output_schema,
            "describe_row",
            "SCALAR",
        )),
        |batch, _, output_field| {
            UDFProcessor::new(batch).process_unary::<StructValue, String, _>(output_field, |row| {
                let name = match row.get("name") {
                    Some(ScalarValue::String(name)) => name.clone(),
                    _ => "missing".to_string(),
                };
                let size = match row.get("size") {
                    Some(ScalarValue::Int64(size)) => *size,
                    _ => 0,
                };
                format!("{name}:{size}")
            })
        },
    )
    .await
    .unwrap();

    let batches = read_response_batches(&value);
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(result.value(0), "athena:7");
    assert_eq!(result.value(1), "missing:9");
}

#[tokio::test]
async fn struct_output_matches_the_declared_row_type() {
    let (input_schema, records) = string_input(vec![Some("athena"), Some("lambda")]);
    let output_schema = Schema::new(vec![Field::new(
        "result",
        DataType::Struct(Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("length", DataType::Int64, true),
        ])),
        true,
    )]);

    let value = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "to_row",
            "SCALAR",
        )),
        |batch, _, output_field| {
            UDFProcessor::new(batch).process_unary::<String, StructValue, _>(output_field, |text| {
                StructValue::new([
                    ("name", text.clone().into()),
                    ("length", ScalarValue::Int64(text.len() as i64)),
                ])
            })
        },
    )
    .await
    .unwrap();

    let batches = read_response_batches(&value);
    assert_eq!(batches[0].schema(), Arc::new(output_schema));

    let rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(rows.len(), 2);
    let names = rows
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let lengths = rows
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(names.value(0), "athena");
    assert_eq!(lengths.value(0), 6);
    assert_eq!(names.value(1), "lambda");
    assert_eq!(lengths.value(1), 6);
}

#[tokio::test]
async fn null_struct_rows_keep_the_declared_type() {
    let (input_schema, records) = string_input(vec![None, None]);
    let output_schema = Schema::new(vec![Field::new(
        "result",
        DataType::Struct(Fields::from(vec![Field::new("name", DataType::Utf8, true)])),
        true,
    )]);

    let value = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "to_row",
            "SCALAR",
        )),
        |batch, _, output_field| {
            UDFProcessor::new(batch).process_unary::<String, StructValue, _>(output_field, |text| {
                StructValue::new([("name", text.into())])
            })
        },
    )
    .await
    .unwrap();

    let batches = read_response_batches(&value);
    assert_eq!(batches[0].schema(), Arc::new(output_schema));
    let rows = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(rows.is_null(0));
    assert!(rows.is_null(1));
}

#[tokio::test]
async fn empty_output_schema_is_an_error() {
    let (input_schema, records) = string_input(vec![Some("x")]);
    let output_schema = Schema::empty();

    let result = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "identity",
            "SCALAR",
        )),
        |batch, _, output_field| {
            UDFProcessor::new(batch).process_unary::<String, String, _>(output_field, |value| value)
        },
    )
    .await;

    let error = result.unwrap_err().to_string();
    assert!(error.contains("output schema has no fields"));
}

#[tokio::test]
async fn arity_mismatch_is_an_error() {
    let (input_schema, records) = string_input(vec![Some("x")]);
    let output_schema = Schema::new(vec![Field::new("result", DataType::Utf8, true)]);

    let result = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "add",
            "SCALAR",
        )),
        |batch, _, output_field| {
            UDFProcessor::new(batch)
                .process_binary::<String, String, String, _>(output_field, |a, b| a + &b)
        },
    )
    .await;

    let error = result.unwrap_err().to_string();
    assert!(error.contains("1 input columns, but the function expects 2"));
}

#[tokio::test]
async fn declared_type_mismatch_is_an_error() {
    let (input_schema, records) = string_input(vec![Some("x")]);
    let output_schema = Schema::new(vec![Field::new("result", DataType::Float64, true)]);

    let result = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "length",
            "SCALAR",
        )),
        |batch, _, output_field| {
            UDFProcessor::new(batch)
                .process_unary::<String, i64, _>(output_field, |value| value.len() as i64)
        },
    )
    .await;

    let error = result.unwrap_err().to_string();
    assert!(error.contains("declared output type Float64"));
}

#[tokio::test]
async fn non_scalar_function_type_is_an_error() {
    let (input_schema, records) = string_input(vec![Some("x")]);
    let output_schema = Schema::new(vec![Field::new("result", DataType::Utf8, true)]);

    let result = handle_athena_request(
        event(request(
            &input_schema,
            &records,
            &output_schema,
            "identity",
            "AGGREGATE",
        )),
        |_, _, _| unreachable!(),
    )
    .await;

    let error = result.unwrap_err().to_string();
    assert!(error.contains("Unsupported functionType: AGGREGATE"));
}

#[tokio::test]
async fn unknown_request_type_is_an_error() {
    let result = handle_athena_request(
        event(serde_json::json!({"@type": "SomethingElse"})),
        |_, _, _| unreachable!(),
    )
    .await;

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Unknown request type"));
}
