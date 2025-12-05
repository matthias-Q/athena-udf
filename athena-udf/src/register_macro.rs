/// Declarative macro for registering multiple Athena UDFs with their type signatures.
///
/// This macro generates a complete async function_handler that routes Athena UDF calls
/// to the appropriate processor based on the function name and type signature.
///
/// # Syntax
///
/// ```ignore
/// athena_udf_handler! {
///     "function_name" => function_ident: (InputType1, InputType2, ...) -> OutputType,
///     ...
/// }
/// ```
///
/// The macro will:
/// 1. Generate the complete `async fn function_handler` function
/// 2. Determine the arity (number of inputs) from the type signature
/// 3. Select the appropriate process method (process_unary, process_binary, etc.)
/// 4. Generate the match arm with correct type parameters
///
/// # Examples
///
/// ```ignore
/// use athena_udf::*;
///
/// fn string_reverse(s: String) -> String {
///     s.chars().rev().collect()
/// }
///
/// fn add_numbers(a: i64, b: i64) -> i64 {
///     a + b
/// }
///
/// athena_udf_handler! {
///     "string_reverse" => string_reverse: (String) -> String,
///     "add_numbers" => add_numbers: (i64, i64) -> i64,
/// }
///
/// #[tokio::main]
/// async fn main() -> Result<(), Error> {
///     tracing_subscriber::fmt()
///         .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
///         .init();
///
///     run(service_fn(function_handler)).await
/// }
/// ```
#[macro_export]
macro_rules! athena_udf_handler {
    (
        $( $name:literal => $fn:ident : ( $($input:ty),+ ) -> $output:ty ),+ $(,)?
    ) => {
        async fn function_handler(
            event: $crate::LambdaEvent<$crate::Value>
        ) -> Result<$crate::Value, lambda_runtime::Error> {
            $crate::handle_athena_request(event, |input_batch, method_name, output_col_name| {
                match method_name {
                    $(
                        $name => {
                            $crate::athena_udf_handler!(@process input_batch, output_col_name, $fn, ($($input),+), $output)
                        }
                    )+
                    _ => Err(format!("Unknown function: {}", method_name).into()),
                }
            }).await
        }
    };

    // Process unary functions (1 input)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_unary::<$i1, $output, _>($output_col, $fn)
    };

    // Process binary functions (2 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_binary::<$i1, $i2, $output, _>($output_col, $fn)
    };

    // Process ternary functions (3 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_ternary::<$i1, $i2, $i3, $output, _>($output_col, $fn)
    };

    // Process quaternary functions (4 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty, $i4:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_quaternary::<$i1, $i2, $i3, $i4, $output, _>($output_col, $fn)
    };

    // Process quinary functions (5 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty, $i4:ty, $i5:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_quinary::<$i1, $i2, $i3, $i4, $i5, $output, _>($output_col, $fn)
    };

    // Process senary functions (6 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty, $i4:ty, $i5:ty, $i6:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_senary::<$i1, $i2, $i3, $i4, $i5, $i6, $output, _>($output_col, $fn)
    };
}

/// Lower-level macro for registering UDFs inside a closure.
///
/// Use this when you need more control over the function_handler definition,
/// or when you want to add custom logic before/after UDF dispatch.
///
/// For most cases, prefer `athena_udf_handler!` which generates the complete handler.
///
/// # Syntax
///
/// ```ignore
/// register_udfs!(input_batch, method_name, output_col_name => {
///     "function_name" => function_ident: (InputType1, InputType2, ...) -> OutputType,
///     ...
/// })
/// ```
#[macro_export]
macro_rules! register_udfs {
    // Entry point: processes all function registrations
    (
        $batch:expr, $method:expr, $output_col:expr => {
            $( $name:literal => $fn:ident : ( $($input:ty),+ ) -> $output:ty ),+ $(,)?
        }
    ) => {
        match $method {
            $(
                $name => {
                    $crate::register_udfs!(@process $batch, $output_col, $fn, ($($input),+), $output)
                }
            )+
            _ => Err(format!("Unknown function: {}", $method).into()),
        }
    };

    // Process unary functions (1 input)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_unary::<$i1, $output, _>($output_col, $fn)
    };

    // Process binary functions (2 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_binary::<$i1, $i2, $output, _>($output_col, $fn)
    };

    // Process ternary functions (3 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_ternary::<$i1, $i2, $i3, $output, _>($output_col, $fn)
    };

    // Process quaternary functions (4 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty, $i4:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_quaternary::<$i1, $i2, $i3, $i4, $output, _>($output_col, $fn)
    };

    // Process quinary functions (5 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty, $i4:ty, $i5:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_quinary::<$i1, $i2, $i3, $i4, $i5, $output, _>($output_col, $fn)
    };

    // Process senary functions (6 inputs)
    (@process $batch:expr, $output_col:expr, $fn:ident, ($i1:ty, $i2:ty, $i3:ty, $i4:ty, $i5:ty, $i6:ty), $output:ty) => {
        $crate::UDFProcessor::new($batch)
            .process_senary::<$i1, $i2, $i3, $i4, $i5, $i6, $output, _>($output_col, $fn)
    };
}

#[cfg(test)]
mod tests {
    use crate::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn string_reverse(s: String) -> String {
        s.chars().rev().collect()
    }

    fn add_numbers(a: i64, b: i64) -> i64 {
        a + b
    }

    fn concat_three(a: String, b: String, c: String) -> String {
        format!("{}{}{}", a, b, c)
    }

    #[test]
    fn test_register_udfs_unary() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
        let input_array = StringArray::from(vec![Some("hello"), Some("world")]);
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let method_name = "string_reverse";
        let output_col_name = "output";

        let result = register_udfs!(&input_batch, method_name, output_col_name => {
            "string_reverse" => string_reverse: (String) -> String,
        });

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(output_array.value(0), "olleh");
        assert_eq!(output_array.value(1), "dlrow");
    }

    #[test]
    fn test_register_udfs_binary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let a_array = Int64Array::from(vec![Some(10), Some(20)]);
        let b_array = Int64Array::from(vec![Some(5), Some(15)]);
        let input_batch =
            RecordBatch::try_new(schema, vec![Arc::new(a_array), Arc::new(b_array)]).unwrap();

        let method_name = "add_numbers";
        let output_col_name = "output";

        let result = register_udfs!(&input_batch, method_name, output_col_name => {
            "add_numbers" => add_numbers: (i64, i64) -> i64,
        });

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(output_array.value(0), 15);
        assert_eq!(output_array.value(1), 35);
    }

    #[test]
    fn test_register_udfs_multiple_functions() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
        let input_array = StringArray::from(vec![Some("test")]);
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let method_name = "string_reverse";
        let output_col_name = "output";

        let result = register_udfs!(&input_batch, method_name, output_col_name => {
            "string_reverse" => string_reverse: (String) -> String,
            "add_numbers" => add_numbers: (i64, i64) -> i64,
        });

        assert!(result.is_ok());
    }

    #[test]
    fn test_register_udfs_unknown_function() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
        let input_array = StringArray::from(vec![Some("test")]);
        let input_batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let method_name = "unknown_function";
        let output_col_name = "output";

        let result = register_udfs!(&input_batch, method_name, output_col_name => {
            "string_reverse" => string_reverse: (String) -> String,
        });

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown function"));
    }

    #[test]
    fn test_register_udfs_ternary() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let a_array = StringArray::from(vec![Some("Hello")]);
        let b_array = StringArray::from(vec![Some(" ")]);
        let c_array = StringArray::from(vec![Some("World")]);
        let input_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(a_array), Arc::new(b_array), Arc::new(c_array)],
        )
        .unwrap();

        let method_name = "concat_three";
        let output_col_name = "output";

        let result = register_udfs!(&input_batch, method_name, output_col_name => {
            "concat_three" => concat_three: (String, String, String) -> String,
        });

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(output_array.value(0), "Hello World");
    }

    // Tests for athena_udf_handler! macro
    // Note: These are compile-time tests, ensuring the macro generates valid code

    #[test]
    fn test_athena_udf_handler_macro_compiles() {
        // This test verifies the macro generates valid code that compiles
        #[allow(dead_code)]
        mod inner {
            use super::*;

            fn test_unary(s: String) -> String {
                s.to_uppercase()
            }

            fn test_binary(a: i64, b: i64) -> i64 {
                a + b
            }

            // This generates a function_handler
            athena_udf_handler! {
                "test_unary" => test_unary: (String) -> String,
                "test_binary" => test_binary: (i64, i64) -> i64,
            }
        }
    }
}
