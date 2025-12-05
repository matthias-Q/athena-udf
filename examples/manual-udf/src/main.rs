use athena_udf::*;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::Value;

/// Reverses a string
/// SQL: USING EXTERNAL FUNCTION string_reverse(input VARCHAR) RETURNS VARCHAR
pub fn string_reverse(value: String) -> String {
    value.chars().rev().collect()
}

/// Adds two numbers
/// SQL: USING EXTERNAL FUNCTION add_numbers(a BIGINT, b BIGINT) RETURNS BIGINT
pub fn add_numbers(a: i64, b: i64) -> i64 {
    a + b
}

/// Multiplies two numbers
/// SQL: USING EXTERNAL FUNCTION multiply(a BIGINT, b BIGINT) RETURNS BIGINT
pub fn multiply(a: i64, b: i64) -> i64 {
    a * b
}

/// Concatenates three strings
/// SQL: USING EXTERNAL FUNCTION concat_three(a VARCHAR, b VARCHAR, c VARCHAR) RETURNS VARCHAR
pub fn concat_three(a: String, b: String, c: String) -> String {
    format!("{}{}{}", a, b, c)
}

/// Converts string to uppercase, returns None if length < 3
/// SQL: USING EXTERNAL FUNCTION uppercase_filtered(input VARCHAR) RETURNS VARCHAR
pub fn uppercase_filtered(value: String) -> Option<String> {
    if value.len() >= 3 {
        Some(value.to_uppercase())
    } else {
        None
    }
}

/// Manually implemented function_handler without using the macro.
/// This gives you full control over the request handling logic.
async fn function_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    handle_athena_request(event, |input_batch, method_name, output_col_name| {
        // You can add custom logging or pre-processing here
        tracing::info!("Processing UDF: {}", method_name);

        // Manual match statement with explicit type parameters
        match method_name {
            "string_reverse" => UDFProcessor::new(input_batch)
                .process_unary::<String, String, _>(output_col_name, string_reverse),
            "add_numbers" => UDFProcessor::new(input_batch)
                .process_binary::<i64, i64, i64, _>(output_col_name, add_numbers),
            "multiply" => UDFProcessor::new(input_batch)
                .process_binary::<i64, i64, i64, _>(output_col_name, multiply),
            "concat_three" => UDFProcessor::new(input_batch)
                .process_ternary::<String, String, String, String, _>(
                    output_col_name,
                    concat_three,
                ),
            "uppercase_filtered" => UDFProcessor::new(input_batch)
                .process_unary::<String, Option<String>, _>(output_col_name, uppercase_filtered),
            _ => {
                // Custom error handling
                tracing::error!("Unknown function requested: {}", method_name);
                Err(format!("Unknown function: {}", method_name).into())
            }
        }
    })
    .await
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize tracing for logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Run the Lambda function
    run(service_fn(function_handler)).await
}
