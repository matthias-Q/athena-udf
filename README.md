# athena-udf

[![CI](https://github.com/matthias-Q/athena-udf/actions/workflows/ci.yml/badge.svg)](https://github.com/matthias-Q/athena-udf/actions/workflows/ci.yml)
[![Publish to
crates.io](https://github.com/matthias-Q/athena-udf/actions/workflows/publish.yml/badge.svg)](https://github.com/matthias-Q/athena-udf/actions/workflows/publish.yml)

A Rust crate for building AWS Athena User Defined Functions (UDFs) using AWS
Lambda and Apache Arrow.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
athena-udf = "0.1.0"
lambda_runtime = "0.13"
tokio = { version = "1", features = ["macros"] }
serde_json = "1.0"
tracing-subscriber = "0.3"
```

## Quick Start

### 1. Create your Lambda handler

We provide a simple macro to generate the Lambda function. Use
`athena_udf_handler!` and register the UDFs in there with the name and the
type.

```rust
use athena_udf::*;
use lambda_runtime::{service_fn, run, LambdaEvent, Error};
use serde_json::Value;

/// SQL: USING EXTERNAL FUNCTION string_reverse(input VARCHAR) RETURNS VARCHAR
pub fn string_reverse(value: String) -> String {
    value.chars().rev().collect()
}

/// SQL: USING EXTERNAL FUNCTION add_numbers(a BIGINT, b BIGINT) RETURNS BIGINT
pub fn add_numbers(a: i64, b: i64) -> i64 {
    a + b
}

athena_udf_handler! {
    "string_reverse" => string_reverse: (String) -> String,
    "add_numbers" => add_numbers: (i64, i64) -> i64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .init();

    run(service_fn(function_handler)).await
}
```

The `athena_udf_handler!` macro generates the complete `async fn function_handler` for you. It:
- Automatically determines the function arity (unary, binary, ternary, etc.) from the type signature
- Selects the correct `process_*` method
- Handles all type conversions between Athena and Rust types
- Provides error handling for unknown functions

For more control, you can use the lower-level `register_udfs!` macro or manually implement the handler.

### 2. Deploy to AWS Lambda

Build for Lambda (Amazon Linux 2):

```bash
cargo lambda build --release --arm64
```

Deploy:

```bash
cargo lambda deploy
```

### 3. Use in Athena

```sql
USING EXTERNAL FUNCTION string_reverse(input VARCHAR) RETURNS VARCHAR
LAMBDA 'your-lambda-function-name'

SELECT string_reverse('hello-athena') as reversed;
-- Result: 'anehta-olleh'
```

## Examples

### [Simple UDF Example](https://github.com/matthias-Q/athena-udf/tree/main/examples/simple-udf) (Recommended)

Uses the `athena_udf_handler!` macro for minimal boilerplate:
- Multiple UDF implementations (unary, binary, ternary functions)
- Clean, concise syntax
- Explicit null handling with `Option<T>`
- Deployment instructions

### [Manual UDF Example](https://github.com/matthias-Q/athena-udf/tree/main/examples/manual-udf)

Manual implementation without macros for full control:
- Same UDF implementations as simple-udf
- Custom logging and error handling
- Explicit type parameters
- Useful when you need preprocessing or performance monitoring

Both examples demonstrate the same functionality - choose based on your needs for control vs. simplicity.

## Supported Types

The crate supports automatic conversion between Athena SQL types and Rust
types:

| Athena SQL Type | Rust Type |
|-----------------|-----------|
| VARCHAR         | String    |
| BIGINT          | i64       |
| INTEGER         | i32       |
| DOUBLE          | f64       |
| BOOLEAN         | bool      |
| VARBINARY       | Vec<u8>   |

## Null Handling

The crate provides two ways to handle null values:

### Automatic Null Handling (Default)

When you use unwrapped types like `String` or `i64`, nulls are automatically handled:
- Null inputs are skipped (your function is not called)
- Null outputs are produced for those rows

```rust
pub fn string_reverse(value: String) -> String {
    value.chars().rev().collect()
    // Never receives null, never needs to handle it
}
```

### Explicit Null Handling

Use `Option<T>` to explicitly handle nulls:

```rust
pub fn string_reverse_nullable(value: String) -> Option<String> {
    if value.length() > 3 {
        Some(value)
    } else {
        None
    }
}
```


## Thanks

This project was develop while working at [Unite](https://www.unite.eu) .

## License

Licensed under MIT license ([LICENSE-MIT](LICENSE-MIT)
http://opensource.org/licenses/MIT)

