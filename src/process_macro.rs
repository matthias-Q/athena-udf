#![allow(unused_assignments)]
#![allow(nonstandard_style)]
#![allow(non_snake_case)]
use arrow::record_batch::RecordBatch;

/// Generates process methods for UDF execution with varying numbers of input parameters.
///
/// This macro creates methods that:
/// 1. Extract input columns from a RecordBatch and downcast them to the appropriate Arrow array types
/// 2. Iterate through rows, converting Arrow values to Rust types using `FromArrow`
/// 3. Apply a user-provided function to the converted values
/// 4. Convert results back to Arrow arrays using `ToArrow`
/// 5. Return a new RecordBatch with the output column
///
/// # Arguments
///
/// * `$method` - The name of the generated method (e.g., `process_unary`, `process_binary`)
/// * `$($input:ident),+` - Generic type parameters for input types
/// * `$output` - Generic type parameter for the output type
///
/// # Generated Method Signature
///
/// ```ignore
/// pub fn $method<I1, I2, ..., O, F>(
///     &self,
///     output_field_name: &str,
///     user_fn: F,
/// ) -> Result<RecordBatch, lambda_runtime::Error>
/// where
///     I1: FromArrow,
///     I2: FromArrow,
///     ...
///     O: ToArrow,
///     F: Fn(I1, I2, ...) -> O,
/// ```
///
/// # Null Handling
///
/// If any input value is null, the result for that row will be null.
#[macro_export]
macro_rules! impl_process {
    ($method:ident, $($input:ident),+; $output:ident) => {
        pub fn $method<$($input,)+ $output, F>(
            &self,
            output_field_name: &str,
            user_fn: F,
        ) -> Result<RecordBatch, lambda_runtime::Error>
        where
            $($input: $crate::FromArrow,)+
            $output: $crate::ToArrow,
            F: Fn($($input),+) -> $output,
        {
            #[allow(unused_mut)]
            let mut col_idx = 0;
            $(
                #[allow(non_snake_case)]
                let $input = self.batch.column(col_idx)
                    .as_any()
                    .downcast_ref::<$input::ArrayType>()
                    .ok_or(format!("Column {} type mismatch", col_idx))?;
                col_idx += 1;
            )+

            let num_rows = self.batch.num_rows();
            let mut results = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                let result = match (
                    $($input::from_array($input, row_idx),)+
                ) {
                    ($(Some($input),)+) => Some(user_fn($($input),+)),
                    _ => None,
                };
                results.push(result);
            }

            let output_array = $output::to_array(results);
            let output_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(output_field_name, $output::data_type(), true),
            ]));

            Ok(RecordBatch::try_new(output_schema, vec![output_array])?)
        }
    };
}

/// Processes Arrow RecordBatches by applying user-defined functions to each row.
///
/// `UdfProcessor` provides methods for processing 1-6 input columns, converting
/// Arrow data to Rust types, applying transformations, and converting back to Arrow format.
///
/// # Examples
///
/// ```
/// # use arrow::array::{Int64Array, StringArray};
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use arrow::record_batch::RecordBatch;
/// # use std::sync::Arc;
/// # use athena_udf::process_macro::UDFProcessor;
/// # use arrow::array::Array;
/// // Unary function: convert i64 to string
/// let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Int64, true)]));
/// let input_array = Int64Array::from(vec![Some(42), None, Some(100)]);
/// let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();
///
/// let processor = UDFProcessor::new(&batch);
/// let result = processor.process_unary::<i64, String, _>("output", |n| format!("Number: {}", n));
///
/// assert!(result.is_ok());
/// let output_batch = result.unwrap();
/// let output_array = output_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
/// assert_eq!(output_array.value(0), "Number: 42");
/// assert!(output_array.is_null(1));
/// ```
pub struct UDFProcessor<'a> {
    batch: &'a RecordBatch,
}

impl<'a> UDFProcessor<'a> {
    /// Creates a new UdfProcessor for the given RecordBatch.
    ///
    /// # Arguments
    ///
    /// * `batch` - The RecordBatch to process
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow::array::Int64Array;
    /// # use arrow::datatypes::{DataType, Field, Schema};
    /// # use arrow::record_batch::RecordBatch;
    /// # use std::sync::Arc;
    /// # use athena_udf::process_macro::UDFProcessor;
    /// let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, true)]));
    /// let array = Int64Array::from(vec![1, 2, 3]);
    /// let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
    ///
    /// let processor = UDFProcessor::new(&batch);
    /// ```
    pub fn new(batch: &'a RecordBatch) -> Self {
        Self { batch }
    }

    impl_process!(process_unary, I1; O);
    impl_process!(process_binary, I1, I2; O);
    impl_process!(process_ternary, I1, I2, I3; O);
    impl_process!(process_quaternary, I1, I2, I3, I4; O);
    impl_process!(process_quinary, I1, I2, I3, I4, I5; O);
    impl_process!(process_senary, I1, I2, I3, I4, I5, I6; O);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_process_unary_string() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));

        let input_array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result = processor.process_unary::<String, String, _>("output", |s| s.to_uppercase());

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        assert_eq!(output_batch.num_rows(), 3);

        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(output_array.value(0), "HELLO");
        assert!(output_array.is_null(1));
        assert_eq!(output_array.value(2), "WORLD");
    }

    #[test]
    fn test_process_unary_i64_to_string() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "input",
            DataType::Int64,
            true,
        )]));

        let input_array = Int64Array::from(vec![Some(42), None, Some(100)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result =
            processor.process_unary::<i64, String, _>("output", |n| format!("Number: {}", n));

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(output_array.value(0), "Number: 42");
        assert!(output_array.is_null(1));
        assert_eq!(output_array.value(2), "Number: 100");
    }

    #[test]
    fn test_process_binary_add() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));

        let a_array = Int64Array::from(vec![Some(10), Some(20), None, Some(30)]);
        let b_array = Int64Array::from(vec![Some(5), None, Some(15), Some(25)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(a_array), Arc::new(b_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result = processor.process_binary::<i64, i64, i64, _>("sum", |a, b| a + b);

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(output_array.value(0), 15);
        assert!(output_array.is_null(1));
        assert!(output_array.is_null(2));
        assert_eq!(output_array.value(3), 55);
    }

    #[test]
    fn test_process_ternary_concat() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]));

        let a_array = StringArray::from(vec![Some("Hello"), None]);
        let b_array = StringArray::from(vec![Some(" "), Some("World")]);
        let c_array = StringArray::from(vec![Some("World"), Some("!")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(a_array), Arc::new(b_array), Arc::new(c_array)],
        )
        .unwrap();

        let processor = UDFProcessor::new(&batch);
        let result = processor
            .process_ternary::<String, String, String, String, _>("result", |a, b, c| {
                format!("{}{}{}", a, b, c)
            });

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(output_array.value(0), "Hello World");
        assert!(output_array.is_null(1));
    }

    #[test]
    fn test_option_string_with_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
        let input_array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);

        let result = processor
            .process_unary::<Option<String>, Option<String>, _>("output", |opt_s| {
                opt_s.map(|s| s.to_uppercase())
            });

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(output_array.value(0), "HELLO");
        assert!(output_array.is_null(1));
        assert_eq!(output_array.value(2), "WORLD");
    }

    #[test]
    fn test_string_to_optional_i64() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
        let input_array = StringArray::from(vec![Some("42"), Some("invalid"), Some("100")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result =
            processor.process_unary::<String, Option<i64>, _>("output", |s| s.parse::<i64>().ok());

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(output_array.value(0), 42);
        assert!(output_array.is_null(1)); // "invalid" -> None -> null
        assert_eq!(output_array.value(2), 100);
    }
}
