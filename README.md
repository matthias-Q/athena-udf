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
athena-udf = "0.3"
lambda_runtime = "1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tracing-subscriber = "0.3"
```

The crate requires Rust 1.88 or newer.

## Quick Start

### 1. Create your Lambda handler

We provide a simple macro to generate the Lambda function. Use
`athena_udf_handler!` and register the UDFs in there with the name and the
type.

```rust
use athena_udf::*;
use lambda_runtime::{service_fn, run, Error};

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

Athena invokes the function with its own IAM role. The crate reads the
`identity` block but does not authorize it. Do not expose the function through a
public function URL or an unauthenticated API Gateway route, or any caller can
run your UDF and read its results.

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
| ROW(...)        | `StructValue` |
| ARRAY<T>        | `ListValue` |

Every type is available as `Option<T>` for explicit null handling.

## Struct Values

A `ROW(...)` column arrives as an Arrow struct. Use `StructValue` to read its
fields and to build a new one:

```rust
use athena_udf::{ScalarValue, StructValue};

/// SQL: USING EXTERNAL FUNCTION label(r ROW(name VARCHAR, size BIGINT)) RETURNS VARCHAR
pub fn label(row: StructValue) -> String {
    let name = match row.get("name") {
        Some(ScalarValue::String(name)) => name.as_str(),
        _ => "unknown",
    };
    let size = match row.get("size") {
        Some(ScalarValue::Int64(size)) => *size,
        _ => 0,
    };
    format!("{name} ({size})")
}

/// SQL: USING EXTERNAL FUNCTION to_row(value VARCHAR) RETURNS ROW(value VARCHAR, length BIGINT)
pub fn to_row(value: String) -> StructValue {
    StructValue::new([
        ("value", value.clone().into()),
        ("length", ScalarValue::Int64(value.len() as i64)),
    ])
}

athena_udf_handler! {
    "label" => label: (StructValue) -> String,
    "to_row" => to_row: (String) -> StructValue,
}
```

The output column is built from the `ROW` type that Athena declares, so field
names and field types in the response always match the SQL signature. Field
lookup is case-insensitive so it matches Athena's lowercased `ROW` field names.
A struct value with a missing, undeclared, or duplicate field is rejected with
an error.

`ScalarValue` supports the same types as the table above, including nested
structs and lists. `ScalarValue::Null` marks a null field inside a row.

## List Values

An `ARRAY<T>` column arrives as an Arrow list. Use `ListValue` to read it and to
build a new one. This covers `ARRAY(ROW(...))`, the shape most multi-row UDFs
return:

```rust
use athena_udf::{ListValue, ScalarValue, StructValue};

/// SQL: USING EXTERNAL FUNCTION units(text VARCHAR)
///      RETURNS ARRAY(ROW(amount VARCHAR, unit VARCHAR))
pub fn units(text: String) -> ListValue {
    text.split_whitespace()
        .filter_map(|word| word.split_once('.'))
        .map(|(amount, unit)| {
            StructValue::new([
                ("amount", amount.into()),
                ("unit", unit.into()),
            ])
        })
        .collect()
}
```

The element type comes from the declared `ARRAY` type of the request, so one
Rust type covers every list the SQL can spell. Null rows stay null, and
`ScalarValue::Null` marks a null element.

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
    if value.len() > 3 {
        Some(value)
    } else {
        None
    }
}
```

A row is never dropped. A null input, or a `None` output, produces a null in
the result column at the same position.


## Thanks

This project was develop while working at [Unite](https://www.unite.eu) .

## License

Licensed under the MIT license ([LICENSE](LICENSE)
https://opensource.org/licenses/MIT)

