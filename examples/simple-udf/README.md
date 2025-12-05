# Simple UDF Example

This example demonstrates how to create AWS Athena User Defined Functions (UDFs) using the `athena-udf` crate with the `athena_udf_handler!` macro.

## Overview

The example includes several UDF implementations:

1. **string_reverse** - Reverses a string
2. **add_numbers** - Adds two integers
3. **multiply** - Multiplies two integers
4. **concat_three** - Concatenates three strings
5. **uppercase_filtered** - Converts to uppercase, returns NULL if length < 3

## Building

Build the example for AWS Lambda (Amazon Linux 2):

```bash
# Install cargo-lambda if you haven't already
cargo install cargo-lambda

# Build for Lambda (ARM64)
cargo lambda build --release --arm64 -p simple-udf

# Or for x86_64
cargo lambda build --release -p simple-udf
```

## Deployment

Deploy to AWS Lambda:

```bash
cargo lambda deploy simple-udf
```

Or manually:
1. Find the binary in `target/lambda/simple-udf/bootstrap.zip`
2. Create a Lambda function with the custom runtime
3. Upload the ZIP file

## Configuration

Lambda settings:
- **Runtime**: Custom runtime (Amazon Linux 2)
- **Architecture**: arm64 (or x86_64 depending on your build)
- **Handler**: Not applicable for custom runtime
- **Timeout**: 60 seconds (or adjust based on your needs)
- **Memory**: 256 MB (adjust as needed)

## Using in Athena

Once deployed, you can use the functions in Athena:

```sql
-- Register the functions
USING EXTERNAL FUNCTION string_reverse(input VARCHAR) RETURNS VARCHAR
LAMBDA 'simple-udf'

USING EXTERNAL FUNCTION add_numbers(a BIGINT, b BIGINT) RETURNS BIGINT
LAMBDA 'simple-udf'

USING EXTERNAL FUNCTION multiply(a BIGINT, b BIGINT) RETURNS BIGINT
LAMBDA 'simple-udf'

USING EXTERNAL FUNCTION concat_three(a VARCHAR, b VARCHAR, c VARCHAR) RETURNS VARCHAR
LAMBDA 'simple-udf'

USING EXTERNAL FUNCTION uppercase_filtered(input VARCHAR) RETURNS VARCHAR
LAMBDA 'simple-udf'

-- Use the functions
SELECT 
    string_reverse('hello-athena') as reversed,
    add_numbers(10, 20) as sum,
    multiply(5, 6) as product,
    concat_three('Hello', ' ', 'World') as concatenated,
    uppercase_filtered('hi') as filtered1,  -- Returns NULL
    uppercase_filtered('hello') as filtered2  -- Returns 'HELLO'
;
```

## Code Highlights

### The Magic of `athena_udf_handler!`

Instead of writing boilerplate code, the macro generates everything:

```rust
// Just define your functions
pub fn string_reverse(value: String) -> String {
    value.chars().rev().collect()
}

pub fn add_numbers(a: i64, b: i64) -> i64 {
    a + b
}

// Then register them with the macro
athena_udf_handler! {
    "string_reverse" => string_reverse: (String) -> String,
    "add_numbers" => add_numbers: (i64, i64) -> i64,
}
```

The macro:
- Generates the complete `async fn function_handler`
- Automatically determines arity (unary, binary, ternary, etc.)
- Selects the correct `process_*` method
- Handles type conversions between Athena and Rust types
- Provides proper error handling for unknown functions

### Null Handling

The crate supports both automatic and explicit null handling:

```rust
// Automatic: nulls are skipped, function never sees them
pub fn string_reverse(value: String) -> String { ... }

// Explicit: function can decide what to return
pub fn uppercase_filtered(value: String) -> Option<String> {
    if value.len() >= 3 { Some(value.to_uppercase()) } else { None }
}
```

## Testing Locally

You can test the Lambda function locally with cargo-lambda:

```bash
cargo lambda watch -p simple-udf
```

Then invoke it with a test event (see Athena UDF request format in the main README).
