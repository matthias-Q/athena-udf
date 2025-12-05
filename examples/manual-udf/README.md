# Manual UDF Example (Without Macros)

This example demonstrates how to create AWS Athena User Defined Functions (UDFs) **without** using the `athena_udf_handler!` macro, giving you full control over the function handler implementation.

## Overview

This example shows the same UDF implementations as the `simple-udf` example, but with a manually written `function_handler`:

1. **string_reverse** - Reverses a string
2. **add_numbers** - Adds two integers
3. **multiply** - Multiplies two integers
4. **concat_three** - Concatenates three strings
5. **uppercase_filtered** - Converts to uppercase, returns NULL if length < 3

## When to Use Manual Implementation

Choose manual implementation over the macro when you need:

- **Custom logging** - Log each UDF invocation or specific events
- **Preprocessing** - Validate or transform inputs before processing
- **Conditional routing** - Different processing logic based on runtime conditions
- **Performance monitoring** - Measure execution time per function
- **Error enrichment** - Add custom error messages or context
- **Dynamic behavior** - Function routing based on configuration

## Code Structure

### Manual Handler

```rust
async fn function_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    handle_athena_request(event, |input_batch, method_name, output_col_name| {
        // Custom logging
        tracing::info!("Processing UDF: {}", method_name);
        
        // Explicit match with full type parameters
        match method_name {
            "string_reverse" => {
                UDFProcessor::new(input_batch)
                    .process_unary::<String, String, _>(output_col_name, string_reverse)
            }
            "add_numbers" => {
                UDFProcessor::new(input_batch)
                    .process_binary::<i64, i64, i64, _>(output_col_name, add_numbers)
            }
            // ... more functions
            _ => {
                tracing::error!("Unknown function: {}", method_name);
                Err(format!("Unknown function: {}", method_name).into())
            }
        }
    }).await
}
```

### Comparison with Macro Version

**Manual (this example):**
```rust
async fn function_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    handle_athena_request(event, |input_batch, method_name, output_col_name| {
        tracing::info!("Processing UDF: {}", method_name);
        match method_name {
            "string_reverse" => UDFProcessor::new(input_batch)
                .process_unary::<String, String, _>(output_col_name, string_reverse),
            "add_numbers" => UDFProcessor::new(input_batch)
                .process_binary::<i64, i64, i64, _>(output_col_name, add_numbers),
            _ => Err(format!("Unknown function: {}", method_name).into()),
        }
    }).await
}
```

**Macro version (simple-udf example):**
```rust
athena_udf_handler! {
    "string_reverse" => string_reverse: (String) -> String,
    "add_numbers" => add_numbers: (i64, i64) -> i64,
}
```

The macro version is more concise, but the manual version gives you flexibility to add custom logic.

## Building

Build the example for AWS Lambda (Amazon Linux 2):

```bash
# Install cargo-lambda if you haven't already
cargo install cargo-lambda

# Build for Lambda (ARM64)
cargo lambda build --release --arm64 -p manual-udf

# Or for x86_64
cargo lambda build --release -p manual-udf
```

## Deployment

Deploy to AWS Lambda:

```bash
cargo lambda deploy manual-udf
```

Or manually:
1. Find the binary in `target/lambda/manual-udf/bootstrap.zip`
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
LAMBDA 'manual-udf'

USING EXTERNAL FUNCTION add_numbers(a BIGINT, b BIGINT) RETURNS BIGINT
LAMBDA 'manual-udf'

USING EXTERNAL FUNCTION multiply(a BIGINT, b BIGINT) RETURNS BIGINT
LAMBDA 'manual-udf'

USING EXTERNAL FUNCTION concat_three(a VARCHAR, b VARCHAR, c VARCHAR) RETURNS VARCHAR
LAMBDA 'manual-udf'

USING EXTERNAL FUNCTION uppercase_filtered(input VARCHAR) RETURNS VARCHAR
LAMBDA 'manual-udf'

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

## Process Methods Reference

The `UDFProcessor` provides methods for different arities:

| Method | Parameters | Example |
|--------|-----------|---------|
| `process_unary` | 1 input | `.process_unary::<String, String, _>()` |
| `process_binary` | 2 inputs | `.process_binary::<i64, i64, i64, _>()` |
| `process_ternary` | 3 inputs | `.process_ternary::<String, String, String, String, _>()` |
| `process_quaternary` | 4 inputs | `.process_quaternary::<T1, T2, T3, T4, O, _>()` |
| `process_quinary` | 5 inputs | `.process_quinary::<T1, T2, T3, T4, T5, O, _>()` |
| `process_senary` | 6 inputs | `.process_senary::<T1, T2, T3, T4, T5, T6, O, _>()` |

**Type Parameter Pattern:**
- Input types in order
- Output type last
- Function type (use `_` for inference)

## Advanced Example: Custom Error Handling

```rust
async fn function_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    handle_athena_request(event, |input_batch, method_name, output_col_name| {
        let start = std::time::Instant::now();
        
        let result = match method_name {
            "string_reverse" => UDFProcessor::new(input_batch)
                .process_unary::<String, String, _>(output_col_name, string_reverse),
            _ => Err(format!("Unknown function: {}", method_name).into()),
        };
        
        let duration = start.elapsed();
        tracing::info!("UDF {} completed in {:?}", method_name, duration);
        
        result
    }).await
}
```

## Testing Locally

You can test the Lambda function locally with cargo-lambda:

```bash
cargo lambda watch -p manual-udf
```

Then invoke it with a test event (see Athena UDF request format in the main README).
