use arrow::array::*;
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
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
/// assert_eq!(String::from_array(&array, 0), Some("hello".to_string()));
/// assert_eq!(String::from_array(&array, 1), None);
/// assert_eq!(String::from_array(&array, 2), Some("world".to_string()));
/// ```
pub trait FromArrow: Sized {
    type ArrayType: Array + 'static;

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self>;
    fn array_type() -> DataType;
}

/// Trait for converting from Rust types to Arrow arrays.
///
/// This trait enables conversion of Rust values into Arrow array columns,
/// preserving null values. It is used by the UDF processor to convert
/// output values back into Arrow format.
///
/// # Examples
///
/// ```
/// # use arrow::array::{Array, StringArray};
/// # use athena_udf::arrow_conversions::ToArrow;
/// let values = vec![Some("hello".to_string()), None, Some("world".to_string())];
/// let array = String::to_array(values);
/// let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(string_array.value(0), "hello");
/// assert!(string_array.is_null(1));
/// assert_eq!(string_array.value(2), "world");
/// ```
pub trait ToArrow {
    type ArrayType: Array + 'static;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef
    where
        Self: Sized;
    fn data_type() -> DataType;
}

/// Converts `String` values from Arrow UTF-8 arrays.
///
/// Returns `None` for null values in the array.
impl FromArrow for String {
    type ArrayType = StringArray;

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        if array.is_null(index) {
            None
        } else {
            Some(array.value(index).to_string())
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

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        if array.is_null(index) {
            None
        } else {
            Some(array.value(index))
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

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        if array.is_null(index) {
            None
        } else {
            Some(array.value(index))
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

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        if array.is_null(index) {
            None
        } else {
            Some(array.value(index))
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

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        if array.is_null(index) {
            None
        } else {
            Some(array.value(index))
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

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        if array.is_null(index) {
            None
        } else {
            Some(array.value(index).to_vec())
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
    type ArrayType = StringArray;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    fn data_type() -> DataType {
        DataType::Utf8
    }
}

/// Converts 64-bit signed integers to Arrow Int64 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for i64 {
    type ArrayType = Int64Array;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    fn data_type() -> DataType {
        DataType::Int64
    }
}

/// Converts 32-bit signed integers to Arrow Int32 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for i32 {
    type ArrayType = Int32Array;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    fn data_type() -> DataType {
        DataType::Int32
    }
}

/// Converts 64-bit floating point numbers to Arrow Float64 arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for f64 {
    type ArrayType = Float64Array;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    fn data_type() -> DataType {
        DataType::Float64
    }
}

/// Converts boolean values to Arrow Boolean arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for bool {
    type ArrayType = BooleanArray;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(BooleanArray::from(values))
    }

    fn data_type() -> DataType {
        DataType::Boolean
    }
}

/// Converts binary data to Arrow Binary arrays.
///
/// Preserves `None` values as nulls in the resulting array.
impl ToArrow for Vec<u8> {
    type ArrayType = BinaryArray;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        Arc::new(BinaryArray::from_iter(values))
    }

    fn data_type() -> DataType {
        DataType::Binary
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
/// assert_eq!(Option::<String>::from_array(&array, 0), Some(Some("hello".to_string())));
/// assert_eq!(Option::<String>::from_array(&array, 1), Some(None));
/// ```
impl<T: FromArrow> FromArrow for Option<T> {
    type ArrayType = T::ArrayType;

    fn from_array(array: &Self::ArrayType, index: usize) -> Option<Self> {
        Some(T::from_array(array, index))
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
/// # use athena_udf::arrow_conversions::ToArrow;
/// let values = vec![Some(Some("hello".to_string())), Some(None), None];
/// let array = Option::<String>::to_array(values);
/// let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
///
/// assert_eq!(string_array.value(0), "hello");
/// assert!(string_array.is_null(1));
/// assert!(string_array.is_null(2));
/// ```
impl<T: ToArrow> ToArrow for Option<T> {
    type ArrayType = T::ArrayType;

    fn to_array(values: Vec<Option<Self>>) -> ArrayRef {
        let flattened: Vec<Option<T>> = values.into_iter().map(|opt| opt.flatten()).collect();
        T::to_array(flattened)
    }

    fn data_type() -> DataType {
        T::data_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_from_arrow() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);

        assert_eq!(String::from_array(&array, 0), Some("hello".to_string()));
        assert_eq!(String::from_array(&array, 1), None);
        assert_eq!(String::from_array(&array, 2), Some("world".to_string()));
    }

    #[test]
    fn test_string_to_arrow() {
        let values = vec![Some("hello".to_string()), None, Some("world".to_string())];
        let array = String::to_array(values);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(string_array.value(0), "hello");
        assert!(string_array.is_null(1));
        assert_eq!(string_array.value(2), "world");
    }

    #[test]
    fn test_i64_from_arrow() {
        let array = Int64Array::from(vec![Some(42), None, Some(-100)]);

        assert_eq!(i64::from_array(&array, 0), Some(42));
        assert_eq!(i64::from_array(&array, 1), None);
        assert_eq!(i64::from_array(&array, 2), Some(-100));
    }

    #[test]
    fn test_i64_to_arrow() {
        let values = vec![Some(42i64), None, Some(-100i64)];
        let array = i64::to_array(values);
        let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_array.value(0), 42);
        assert!(int_array.is_null(1));
        assert_eq!(int_array.value(2), -100);
    }

    #[test]
    fn test_i32_from_arrow() {
        let array = Int32Array::from(vec![Some(10), None, Some(20)]);

        assert_eq!(i32::from_array(&array, 0), Some(10));
        assert_eq!(i32::from_array(&array, 1), None);
        assert_eq!(i32::from_array(&array, 2), Some(20));
    }

    #[test]
    fn test_f64_from_arrow() {
        let array = Float64Array::from(vec![Some(3.14), None, Some(-2.71)]);

        assert_eq!(f64::from_array(&array, 0), Some(3.14));
        assert_eq!(f64::from_array(&array, 1), None);
        assert_eq!(f64::from_array(&array, 2), Some(-2.71));
    }

    #[test]
    fn test_f64_to_arrow() {
        let values = vec![Some(3.14), None, Some(-2.71)];
        let array = f64::to_array(values);
        let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(float_array.value(0), 3.14);
        assert!(float_array.is_null(1));
        assert_eq!(float_array.value(2), -2.71);
    }

    #[test]
    fn test_bool_from_arrow() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);

        assert_eq!(bool::from_array(&array, 0), Some(true));
        assert_eq!(bool::from_array(&array, 1), None);
        assert_eq!(bool::from_array(&array, 2), Some(false));
    }

    #[test]
    fn test_bool_to_arrow() {
        let values = vec![Some(true), None, Some(false)];
        let array = bool::to_array(values);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(bool_array.value(0), true);
        assert!(bool_array.is_null(1));
        assert_eq!(bool_array.value(2), false);
    }

    #[test]
    fn test_data_types() {
        assert_eq!(String::array_type(), DataType::Utf8);
        assert_eq!(i64::array_type(), DataType::Int64);
        assert_eq!(i32::array_type(), DataType::Int32);
        assert_eq!(f64::array_type(), DataType::Float64);
        assert_eq!(bool::array_type(), DataType::Boolean);

        assert_eq!(String::data_type(), DataType::Utf8);
        assert_eq!(i64::data_type(), DataType::Int64);
        assert_eq!(i32::data_type(), DataType::Int32);
        assert_eq!(f64::data_type(), DataType::Float64);
        assert_eq!(bool::data_type(), DataType::Boolean);
    }
}
