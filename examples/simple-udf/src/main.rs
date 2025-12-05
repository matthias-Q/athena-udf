use athena_udf::*;
use lambda_runtime::{run, service_fn, Error};

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

// Use the athena_udf_handler! macro to generate the function_handler
athena_udf_handler! {
    "string_reverse" => string_reverse: (String) -> String,
    "add_numbers" => add_numbers: (i64, i64) -> i64,
    "multiply" => multiply: (i64, i64) -> i64,
    "concat_three" => concat_three: (String, String, String) -> String,
    "uppercase_filtered" => uppercase_filtered: (String) -> Option<String>,
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
