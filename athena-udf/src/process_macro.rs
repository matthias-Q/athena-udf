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
/// 4. Convert results back to Arrow arrays using `ToArrow`, for the declared output type
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
///     output_field: &arrow::datatypes::Field,
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
/// The output field comes from the `outputSchema` of the request. It fixes the
/// column name and the Arrow type of the produced batch, and it rejects a UDF
/// whose return type does not match the declared type.
///
/// # Null Handling
///
/// If any input value is null, the result for that row will be null. Give the
/// parameter the type `Option<T>` to receive nulls instead and handle them in
/// the function.
///
/// # Errors
///
/// Returns an error when the batch has fewer columns than the function
/// parameters, when a column has a different Arrow type, or when the output
/// cannot be built for the declared type.
#[macro_export]
macro_rules! impl_process {
    ($method:ident, $($input:ident),+; $output:ident) => {
        pub fn $method<$($input,)+ $output, F>(
            &self,
            output_field: &arrow::datatypes::Field,
            user_fn: F,
        ) -> Result<RecordBatch, lambda_runtime::Error>
        where
            $($input: $crate::FromArrow,)+
            $output: $crate::ToArrow,
            F: Fn($($input),+) -> $output,
        {
            let arity = [$(stringify!($input)),+].len();
            if self.batch.num_columns() < arity {
                return Err(format!(
                    "The record batch has {} input columns, but the function expects {}",
                    self.batch.num_columns(),
                    arity
                )
                .into());
            }

            #[allow(unused_mut)]
            let mut col_idx = 0;
            $(
                #[allow(non_snake_case)]
                let $input = self.batch.column(col_idx)
                    .as_any()
                    .downcast_ref::<$input::ArrayType>()
                    .ok_or_else(|| format!(
                        "Input column {} has type {}, but the function expects {}",
                        col_idx,
                        self.batch.column(col_idx).data_type(),
                        $input::array_type()
                    ))?;
                col_idx += 1;
            )+

            let num_rows = self.batch.num_rows();
            let mut results = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                let result = match ($($input::from_array($input, row_idx)?,)+) {
                    ($(Some($input),)+) => Some(user_fn($($input),+)),
                    _ => None,
                };
                results.push(result);
            }

            let output_array = $output::to_array(results, output_field.data_type())?;
            let output_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    output_field.name().clone(),
                    output_field.data_type().clone(),
                    true,
                ),
            ]));

            Ok(RecordBatch::try_new(output_schema, vec![output_array])?)
        }
    };
}

/// Processes Arrow RecordBatches by applying user-defined functions to each row.
///
/// `UDFProcessor` provides methods for processing 1-6 input columns, converting
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
/// let output_field = Field::new("output", DataType::Utf8, true);
///
/// let processor = UDFProcessor::new(&batch);
/// let result = processor.process_unary::<i64, String, _>(&output_field, |n| format!("Number: {}", n));
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

    fn output_field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
    }

    #[test]
    fn test_process_unary_string() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));

        let input_array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result = processor
            .process_unary::<String, String, _>(&output_field("output", DataType::Utf8), |s| {
                s.to_uppercase()
            });

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
        let result = processor
            .process_unary::<i64, String, _>(&output_field("output", DataType::Utf8), |n| {
                format!("Number: {}", n)
            });

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
        let result = processor
            .process_binary::<i64, i64, i64, _>(&output_field("sum", DataType::Int64), |a, b| {
                a + b
            });

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
        let result = processor.process_ternary::<String, String, String, String, _>(
            &output_field("result", DataType::Utf8),
            |a, b, c| format!("{}{}{}", a, b, c),
        );

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

        let result = processor.process_unary::<Option<String>, Option<String>, _>(
            &output_field("output", DataType::Utf8),
            |opt_s| opt_s.map(|s| s.to_uppercase()),
        );

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
        let result = processor.process_unary::<String, Option<i64>, _>(
            &output_field("output", DataType::Int64),
            |s| s.parse::<i64>().ok(),
        );

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

    #[test]
    fn test_null_input_keeps_row_count_for_optional_output() {
        let schema = Arc::new(Schema::new(vec![Field::new("input", DataType::Utf8, true)]));
        let input_array = StringArray::from(vec![Some("a"), None, Some("c")]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(input_array)]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result = processor.process_unary::<String, Option<String>, _>(
            &output_field("output", DataType::Utf8),
            |value| Some(value.to_uppercase()),
        );

        assert!(result.is_ok());
        let output_batch = result.unwrap();
        assert_eq!(output_batch.num_rows(), 3);

        let output_array = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(output_array.value(0), "A");
        assert!(output_array.is_null(1));
        assert_eq!(output_array.value(2), "C");
    }

    #[test]
    fn test_too_few_input_columns_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(1)]))]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result = processor
            .process_binary::<i64, i64, i64, _>(&output_field("sum", DataType::Int64), |a, b| {
                a + b
            });

        let error = result.unwrap_err().to_string();
        assert!(error.contains("1 input columns, but the function expects 2"));
    }

    #[test]
    fn test_input_column_type_mismatch_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![Some("x")]))])
                .unwrap();

        let processor = UDFProcessor::new(&batch);
        let result =
            processor.process_unary::<i64, i64, _>(&output_field("o", DataType::Int64), |a| a);

        let error = result.unwrap_err().to_string();
        assert!(error.contains("Input column 0 has type Utf8"));
        assert!(error.contains("expects Int64"));
    }

    #[test]
    fn test_declared_output_type_mismatch_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(1)]))]).unwrap();

        let processor = UDFProcessor::new(&batch);
        let result =
            processor.process_unary::<i64, i64, _>(&output_field("o", DataType::Float64), |a| a);

        assert!(result
            .unwrap_err()
            .to_string()
            .contains("declared output type Float64"));
    }
}
