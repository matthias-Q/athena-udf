use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use std::sync::Arc;

/// Trait for converting from Arrow arrays to Rust types.
///
/// This trait enables extraction of Rust values from Arrow array columns,
/// handling null values appropriately. It is used by the UDF processor to
/// convert input columns into native Rust types for processing.
///
/// # Examples
///
/// ```
/// # use arrow::array::{Array, StringArray};
/// # use athena_udf::arrow_conversions::FromArrow;
/// let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
///
/// assert_eq!(String::from_array(&array, 0).unwrap(), Some("hello".to_string()));
/// assert_eq!(String::from_array(&array, 1).unwrap(), None);
/// assert_eq!(String::from_array(&array, 2).unwrap(), Some("world".to_string()));
/// ```
pub trait FromArrow: Sized {
    type ArrayType: Array + 'static;

    /// Reads the value at `index`, or `None` when the slot is null.
    ///
    /// # Errors
    ///
    /// Returns an error when the array holds a type this implementation cannot
    /// represent, for example an unsupported field inside a struct.
    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError>;

    /// The Arrow type this implementation reads.
    fn array_type() -> DataType;
}

/// Trait for converting Rust values into Arrow arrays.
///
/// The array is built for a declared Arrow type. The declared type comes from
/// the `outputSchema` of the request, so the built array always matches what
/// Athena expects to decode.
///
/// # Examples
///
/// ```
/// # use arrow::array::{Array, StringArray};
/// # use arrow::datatypes::DataType;
/// # use athena_udf::arrow_conversions::ToArrow;
/// let values = vec![Some("hello".to_string()), None, Some("world".to_string())];
/// let array = String::to_array(values, &DataType::Utf8).unwrap();
/// let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(string_array.value(0), "hello");
/// assert!(string_array.is_null(1));
/// assert_eq!(string_array.value(2), "world");
/// ```
pub trait ToArrow: Sized {
    /// Builds an array from the values.
    ///
    /// # Errors
    ///
    /// Returns an error when `field_type` does not describe the values this
    /// implementation produces.
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError>;
}

pub(crate) fn expect_type(expected: DataType, field_type: &DataType) -> Result<(), ArrowError> {
    if *field_type == expected {
        Ok(())
    } else {
        Err(ArrowError::InvalidArgumentError(format!(
            "declared output type {field_type}, but the UDF returns {expected}"
        )))
    }
}

/// Converts `String` values from Arrow UTF-8 arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for String {
    type ArrayType = StringArray;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        if array.is_null(index) {
            Ok(None)
        } else {
            Ok(Some(array.value(index).to_string()))
        }
    }

    fn array_type() -> DataType {
        DataType::Utf8
    }
}

/// Converts 64-bit signed integers from Arrow Int64 arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for i64 {
    type ArrayType = Int64Array;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        if array.is_null(index) {
            Ok(None)
        } else {
            Ok(Some(array.value(index)))
        }
    }

    fn array_type() -> DataType {
        DataType::Int64
    }
}

/// Converts 32-bit signed integers from Arrow Int32 arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for i32 {
    type ArrayType = Int32Array;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        if array.is_null(index) {
            Ok(None)
        } else {
            Ok(Some(array.value(index)))
        }
    }

    fn array_type() -> DataType {
        DataType::Int32
    }
}

/// Converts 64-bit floating point numbers from Arrow Float64 arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for f64 {
    type ArrayType = Float64Array;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        if array.is_null(index) {
            Ok(None)
        } else {
            Ok(Some(array.value(index)))
        }
    }

    fn array_type() -> DataType {
        DataType::Float64
    }
}

/// Converts boolean values from Arrow Boolean arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for bool {
    type ArrayType = BooleanArray;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        if array.is_null(index) {
            Ok(None)
        } else {
            Ok(Some(array.value(index)))
        }
    }

    fn array_type() -> DataType {
        DataType::Boolean
    }
}

/// Converts binary data from Arrow Binary arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for Vec<u8> {
    type ArrayType = BinaryArray;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        if array.is_null(index) {
            Ok(None)
        } else {
            Ok(Some(array.value(index).to_vec()))
        }
    }

    fn array_type() -> DataType {
        DataType::Binary
    }
}

/// Converts `String` values to Arrow UTF-8 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for String {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        expect_type(DataType::Utf8, field_type)?;
        Ok(Arc::new(StringArray::from(values)))
    }
}

/// Converts 64-bit signed integers to Arrow Int64 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for i64 {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        expect_type(DataType::Int64, field_type)?;
        Ok(Arc::new(Int64Array::from(values)))
    }
}

/// Converts 32-bit signed integers to Arrow Int32 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for i32 {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        expect_type(DataType::Int32, field_type)?;
        Ok(Arc::new(Int32Array::from(values)))
    }
}

/// Converts 64-bit floating point numbers to Arrow Float64 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for f64 {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        expect_type(DataType::Float64, field_type)?;
        Ok(Arc::new(Float64Array::from(values)))
    }
}

/// Converts boolean values to Arrow Boolean arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for bool {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        expect_type(DataType::Boolean, field_type)?;
        Ok(Arc::new(BooleanArray::from(values)))
    }
}

/// Converts binary data to Arrow Binary arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for Vec<u8> {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        expect_type(DataType::Binary, field_type)?;
        Ok(Arc::new(BinaryArray::from_iter(values)))
    }
}

/// Implements `FromArrow` for `Option<T>` where `T: FromArrow`.
///
/// This allows UDF functions to explicitly handle null values by accepting
/// `Option<T>` parameters instead of unwrapped `T` values.
///
/// When extracting from an Arrow array:
/// - Non-null values become `Some(Some(value))`
/// - Null values become `Some(None)`
///
/// This enables UDFs to distinguish between "value not present" and implement
/// custom null-handling logic.
///
/// # Examples
///
/// ```
/// # use arrow::array::{Array, StringArray};
/// # use athena_udf::arrow_conversions::FromArrow;
/// let array = StringArray::from(vec![Some("hello"), None]);
///
/// // Option<String> preserves the null as Some(None)
/// assert_eq!(Option::<String>::from_array(&array, 0).unwrap(), Some(Some("hello".to_string())));
/// assert_eq!(Option::<String>::from_array(&array, 1).unwrap(), Some(None));
/// ```
impl<T: FromArrow> FromArrow for Option<T> {
    type ArrayType = T::ArrayType;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        Ok(Some(T::from_array(array, index)?))
    }

    fn array_type() -> DataType {
        T::array_type()
    }
}

/// Implements `ToArrow` for `Option<T>` where `T: ToArrow`.
///
/// This allows UDF functions to return `Option<Option<T>>` values,
/// which are flattened to `Option<T>` before conversion to Arrow format.
///
/// The flattening behavior:
/// - `Some(Some(value))` becomes `Some(value)` in the Arrow array
/// - `Some(None)` becomes `None` (null) in the Arrow array
/// - `None` becomes `None` (null) in the Arrow array
///
/// # Examples
///
/// ```
/// # use arrow::array::{Array, StringArray};
/// # use arrow::datatypes::DataType;
/// # use athena_udf::arrow_conversions::ToArrow;
/// let values = vec![Some(Some("hello".to_string())), Some(None), None];
/// let array = Option::<String>::to_array(values, &DataType::Utf8).unwrap();
/// let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(string_array.value(0), "hello");
/// assert!(string_array.is_null(1));
/// assert!(string_array.is_null(2));
/// ```
impl<T: ToArrow> ToArrow for Option<T> {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        // `flatten` on the item, not on the iterator: an outer `None` is a null
        // row, not a row to skip.
        let flattened: Vec<Option<T>> = values.into_iter().map(Option::flatten).collect();
        T::to_array(flattened, field_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_from_arrow() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);

        assert_eq!(
            String::from_array(&array, 0).unwrap(),
            Some("hello".to_string())
        );
        assert_eq!(String::from_array(&array, 1).unwrap(), None);
        assert_eq!(
            String::from_array(&array, 2).unwrap(),
            Some("world".to_string())
        );
    }

    #[test]
    fn test_string_to_arrow() {
        let values = vec![Some("hello".to_string()), None, Some("world".to_string())];
        let array = String::to_array(values, &DataType::Utf8).unwrap();
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(string_array.value(0), "hello");
        assert!(string_array.is_null(1));
        assert_eq!(string_array.value(2), "world");
    }

    #[test]
    fn test_i64_from_arrow() {
        let array = Int64Array::from(vec![Some(42), None, Some(-100)]);

        assert_eq!(i64::from_array(&array, 0).unwrap(), Some(42));
        assert_eq!(i64::from_array(&array, 1).unwrap(), None);
        assert_eq!(i64::from_array(&array, 2).unwrap(), Some(-100));
    }

    #[test]
    fn test_i64_to_arrow() {
        let values = vec![Some(42i64), None, Some(-100i64)];
        let array = i64::to_array(values, &DataType::Int64).unwrap();
        let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_array.value(0), 42);
        assert!(int_array.is_null(1));
        assert_eq!(int_array.value(2), -100);
    }

    #[test]
    fn test_i32_from_arrow() {
        let array = Int32Array::from(vec![Some(10), None, Some(20)]);

        assert_eq!(i32::from_array(&array, 0).unwrap(), Some(10));
        assert_eq!(i32::from_array(&array, 1).unwrap(), None);
        assert_eq!(i32::from_array(&array, 2).unwrap(), Some(20));
    }

    #[test]
    fn test_f64_from_arrow() {
        let array = Float64Array::from(vec![Some(1.5), None, Some(-2.25)]);

        assert_eq!(f64::from_array(&array, 0).unwrap(), Some(1.5));
        assert_eq!(f64::from_array(&array, 1).unwrap(), None);
        assert_eq!(f64::from_array(&array, 2).unwrap(), Some(-2.25));
    }

    #[test]
    fn test_f64_to_arrow() {
        let values = vec![Some(1.5), None, Some(-2.25)];
        let array = f64::to_array(values, &DataType::Float64).unwrap();
        let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(float_array.value(0), 1.5);
        assert!(float_array.is_null(1));
        assert_eq!(float_array.value(2), -2.25);
    }

    #[test]
    fn test_bool_from_arrow() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);

        assert_eq!(bool::from_array(&array, 0).unwrap(), Some(true));
        assert_eq!(bool::from_array(&array, 1).unwrap(), None);
        assert_eq!(bool::from_array(&array, 2).unwrap(), Some(false));
    }

    #[test]
    fn test_bool_to_arrow() {
        let values = vec![Some(true), None, Some(false)];
        let array = bool::to_array(values, &DataType::Boolean).unwrap();
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(bool_array.value(0));
        assert!(bool_array.is_null(1));
        assert!(!bool_array.value(2));
    }

    #[test]
    fn test_declared_output_type_must_match() {
        let values = vec![Some(7_i64)];
        let result = i64::to_array(values, &DataType::Float64);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("declared output type Float64"));
    }
}
