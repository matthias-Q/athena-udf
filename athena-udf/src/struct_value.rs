//! Dynamic values for Arrow struct and list columns (Athena `ROW(...)` and `ARRAY<T>` types).
//!
//! A struct column carries one named field set per row, and a list column
//! carries a variable-length slice of elements per row. [`StructValue`] and
//! [`ListValue`] hold [`ScalarValue`]s, so a UDF can read and build nested
//! columns without a generated Rust type per SQL declaration.
//! # Examples
//!
//! ```
//! # use athena_udf::StructValue;
//! # use athena_udf::ScalarValue;
//! let row = StructValue::new([("name", "athena".into()), ("size", ScalarValue::Int64(3))]);
//! assert_eq!(row.get("name"), Some(&ScalarValue::String("athena".to_string())));
//! ```

use crate::arrow_conversions::{check_bounds, FromArrow, ToArrow};
use arrow::array::{
    as_boolean_array, as_generic_binary_array, as_list_array, as_primitive_array, as_string_array,
    as_struct_array, Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Int64Type};
use arrow::error::ArrowError;
use std::sync::Arc;

/// One value inside a struct column.
///
/// The variants mirror the types the crate converts. `Null` is a value that is
/// present in the row but null in the column.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    String(String),
    Int64(i64),
    Int32(i32),
    Float64(f64),
    Boolean(bool),
    Binary(Vec<u8>),
    Struct(StructValue),
    List(Vec<ScalarValue>),
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for ScalarValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        Self::Int32(value)
    }
}

impl From<f64> for ScalarValue {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<Vec<u8>> for ScalarValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Binary(value)
    }
}

impl From<StructValue> for ScalarValue {
    fn from(value: StructValue) -> Self {
        Self::Struct(value)
    }
}

impl From<Vec<ScalarValue>> for ScalarValue {
    fn from(value: Vec<ScalarValue>) -> Self {
        Self::List(value)
    }
}

impl From<ListValue> for ScalarValue {
    fn from(value: ListValue) -> Self {
        Self::List(value.values)
    }
}

/// A struct column value: an ordered list of named [`ScalarValue`]s.
///
/// Field order follows the order the fields were added. Output columns are
/// built from the declared `ROW` type of the request, so this order does not
/// change what Athena decodes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StructValue {
    fields: Vec<(String, ScalarValue)>,
}

impl StructValue {
    /// Creates a struct value from named fields.
    pub fn new<I, N>(fields: I) -> Self
    where
        I: IntoIterator<Item = (N, ScalarValue)>,
        N: Into<String>,
    {
        Self {
            fields: fields
                .into_iter()
                .map(|(name, value)| (name.into(), value))
                .collect(),
        }
    }

    /// Returns the value of the field with this name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use athena_udf::{ScalarValue, StructValue};
    /// let row = StructValue::new([("id", ScalarValue::Int64(7))]);
    /// assert_eq!(row.get("id"), Some(&ScalarValue::Int64(7)));
    /// assert_eq!(row.get("missing"), None);
    /// ```
    pub fn get(&self, name: &str) -> Option<&ScalarValue> {
        self.fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .or_else(|| {
                self.fields
                    .iter()
                    .find(|(field_name, _)| field_name.eq_ignore_ascii_case(name))
            })
            .map(|(_, value)| value)
    }

    /// Returns the fields in insertion order.
    pub fn fields(&self) -> &[(String, ScalarValue)] {
        &self.fields
    }
}

/// A list column value: an ordered list of [`ScalarValue`]s.
///
/// `ListValue` builds and reads `ARRAY<T>` columns, including
/// `ARRAY<ROW(...)>`. The element type comes from the declared output type of
/// the request, so one Rust type covers every list the SQL can spell.
///
/// # Examples
///
/// ```
/// # use athena_udf::{ListValue, ScalarValue, StructValue};
/// let rows = ListValue::new([
///     ScalarValue::Struct(StructValue::new([("unit", "kg".into())])),
///     ScalarValue::Struct(StructValue::new([("unit", "l".into())])),
/// ]);
/// assert_eq!(rows.values().len(), 2);
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListValue {
    values: Vec<ScalarValue>,
}

impl ListValue {
    /// Creates a list value from its elements.
    pub fn new<I>(values: I) -> Self
    where
        I: IntoIterator<Item = ScalarValue>,
    {
        Self {
            values: values.into_iter().collect(),
        }
    }

    /// Returns the elements in order.
    pub fn values(&self) -> &[ScalarValue] {
        &self.values
    }
}

impl From<Vec<ScalarValue>> for ListValue {
    fn from(values: Vec<ScalarValue>) -> Self {
        Self { values }
    }
}

impl<T: Into<ScalarValue>> FromIterator<T> for ListValue {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            values: iter.into_iter().map(Into::into).collect(),
        }
    }
}

impl IntoIterator for ListValue {
    type Item = ScalarValue;
    type IntoIter = std::vec::IntoIter<ScalarValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<'a> IntoIterator for &'a ListValue {
    type Item = &'a ScalarValue;
    type IntoIter = std::slice::Iter<'a, ScalarValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

fn read_list(array: &ListArray, index: usize) -> Result<Vec<ScalarValue>, ArrowError> {
    let elements = array.value(index);
    let mut values = Vec::with_capacity(elements.len());
    for position in 0..elements.len() {
        let value = read_value(elements.as_ref(), position).map_err(|error| {
            ArrowError::InvalidArgumentError(format!("list element {position}: {error}"))
        })?;
        values.push(value);
    }
    Ok(values)
}

/// Reads one value from any supported array type.
fn read_value(array: &dyn Array, index: usize) -> Result<ScalarValue, ArrowError> {
    if array.is_null(index) {
        return Ok(ScalarValue::Null);
    }

    match array.data_type() {
        DataType::Utf8 => Ok(ScalarValue::String(
            as_string_array(array).value(index).to_string(),
        )),
        DataType::Int64 => Ok(ScalarValue::Int64(
            as_primitive_array::<Int64Type>(array).value(index),
        )),
        DataType::Int32 => Ok(ScalarValue::Int32(
            as_primitive_array::<Int32Type>(array).value(index),
        )),
        DataType::Float64 => Ok(ScalarValue::Float64(
            as_primitive_array::<arrow::datatypes::Float64Type>(array).value(index),
        )),
        DataType::Boolean => Ok(ScalarValue::Boolean(as_boolean_array(array).value(index))),
        DataType::Binary => Ok(ScalarValue::Binary(
            as_generic_binary_array::<i32>(array).value(index).to_vec(),
        )),
        DataType::Struct(_) => match StructValue::from_array(as_struct_array(array), index)? {
            Some(value) => Ok(ScalarValue::Struct(value)),
            None => Ok(ScalarValue::Null),
        },
        DataType::List(_) => Ok(ScalarValue::List(read_list(as_list_array(array), index)?)),
        other => Err(ArrowError::NotYetImplemented(format!(
            "unsupported value type {other}"
        ))),
    }
}

fn variant_name(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Null => "Null",
        ScalarValue::String(_) => "String",
        ScalarValue::Int64(_) => "Int64",
        ScalarValue::Int32(_) => "Int32",
        ScalarValue::Float64(_) => "Float64",
        ScalarValue::Boolean(_) => "Boolean",
        ScalarValue::Binary(_) => "Binary",
        ScalarValue::Struct(_) => "Struct",
        ScalarValue::List(_) => "List",
    }
}

fn mismatch(expected: &str, value: &ScalarValue) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "expected a {expected} value, got {}",
        variant_name(value)
    ))
}

/// Collects one typed column from the struct rows, rejecting nulls and mismatches.
fn collect<'a, T>(
    values: &[Option<&'a ScalarValue>],
    expected: &str,
    pick: impl Fn(&'a ScalarValue) -> Option<T>,
) -> Result<Vec<Option<T>>, ArrowError> {
    values
        .iter()
        .map(|value| match value {
            None | Some(ScalarValue::Null) => Ok(None),
            Some(value) => pick(value)
                .map(Some)
                .ok_or_else(|| mismatch(expected, value)),
        })
        .collect()
}

/// Builds one child array for the declared field type.
fn build_child(
    field_type: &DataType,
    values: &[Option<&ScalarValue>],
) -> Result<ArrayRef, ArrowError> {
    match field_type {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(collect(
            values,
            "String",
            |value| match value {
                ScalarValue::String(text) => Some(text.as_str()),
                _ => None,
            },
        )?))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(collect(
            values,
            "Int64",
            |value| match value {
                ScalarValue::Int64(number) => Some(*number),
                _ => None,
            },
        )?))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(collect(
            values,
            "Int32",
            |value| match value {
                ScalarValue::Int32(number) => Some(*number),
                _ => None,
            },
        )?))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(collect(
            values,
            "Float64",
            |value| match value {
                ScalarValue::Float64(number) => Some(*number),
                _ => None,
            },
        )?))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(collect(
            values,
            "Boolean",
            |value| match value {
                ScalarValue::Boolean(flag) => Some(*flag),
                _ => None,
            },
        )?))),
        DataType::Binary => Ok(Arc::new(BinaryArray::from(collect(
            values,
            "Binary",
            |value| match value {
                ScalarValue::Binary(bytes) => Some(bytes.as_slice()),
                _ => None,
            },
        )?))),
        DataType::Struct(_) => {
            let nested: Vec<Option<&StructValue>> = values
                .iter()
                .map(|value| match value {
                    None | Some(ScalarValue::Null) => Ok(None),
                    Some(ScalarValue::Struct(inner)) => Ok(Some(inner)),
                    Some(other) => Err(mismatch("Struct", other)),
                })
                .collect::<Result<_, _>>()?;
            struct_array(&nested, field_type)
        }
        DataType::List(inner) => {
            let rows: Vec<Option<&Vec<ScalarValue>>> = values
                .iter()
                .map(|value| match value {
                    None | Some(ScalarValue::Null) => Ok(None),
                    Some(ScalarValue::List(items)) => Ok(Some(items)),
                    Some(other) => Err(mismatch("List", other)),
                })
                .collect::<Result<_, _>>()?;
            list_array(&rows, inner)
        }
        other => Err(ArrowError::NotYetImplemented(format!(
            "unsupported value type {other}"
        ))),
    }
}

/// Builds a struct array for the declared struct type.
fn struct_array(
    rows: &[Option<&StructValue>],
    field_type: &DataType,
) -> Result<ArrayRef, ArrowError> {
    let DataType::Struct(fields) = field_type else {
        return Err(ArrowError::InvalidArgumentError(format!(
            "declared output type {field_type} is not a struct"
        )));
    };

    for row in rows.iter().flatten() {
        for (index, (name, _)) in row.fields.iter().enumerate() {
            if row.fields[..index]
                .iter()
                .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
            {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "struct value has a duplicate field \"{name}\""
                )));
            }
            if !fields
                .iter()
                .any(|declared| declared.name().eq_ignore_ascii_case(name))
            {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "struct value has an undeclared field \"{name}\""
                )));
            }
        }
    }

    let mut columns = Vec::with_capacity(fields.len());
    for field in fields.iter() {
        let column: Vec<Option<&ScalarValue>> = rows
            .iter()
            .map(|row| match row {
                Some(value) => value
                    .get(field.name())
                    .map(Some)
                    .ok_or_else(|| missing_field(field.name())),
                None => Ok(None),
            })
            .collect::<Result<_, _>>()?;
        columns.push(build_child(field.data_type(), &column)?);
    }

    let nulls = rows
        .iter()
        .any(Option::is_none)
        .then(|| rows.iter().map(Option::is_some).collect::<NullBuffer>());

    Ok(Arc::new(StructArray::try_new_with_length(
        fields.clone(),
        columns,
        nulls,
        rows.len(),
    )?))
}

fn missing_field(name: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!(
        "struct value is missing the declared field \"{name}\""
    ))
}

/// Builds a list array for the declared list type.
fn list_array(
    rows: &[Option<&Vec<ScalarValue>>],
    inner: &Arc<Field>,
) -> Result<ArrayRef, ArrowError> {
    let mut elements: Vec<Option<&ScalarValue>> = Vec::new();
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0i32);

    for row in rows {
        if let Some(items) = row {
            elements.extend(items.iter().map(|value| match value {
                ScalarValue::Null => None,
                value => Some(value),
            }));
        }
        offsets.push(i32::try_from(elements.len()).map_err(|_| {
            ArrowError::InvalidArgumentError("list value exceeds the i32 offset range".to_string())
        })?);
    }

    let values = build_child(inner.data_type(), &elements)?;
    let nulls = rows
        .iter()
        .any(Option::is_none)
        .then(|| rows.iter().map(Option::is_some).collect::<NullBuffer>());

    Ok(Arc::new(ListArray::try_new(
        inner.clone(),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    )?))
}

impl FromArrow for StructValue {
    type ArrayType = StructArray;

    fn from_array(array: &StructArray, index: usize) -> Result<Option<Self>, ArrowError> {
        check_bounds(array.len(), index)?;
        if array.is_null(index) {
            return Ok(None);
        }

        let mut fields = Vec::with_capacity(array.num_columns());
        for (position, field) in array.fields().iter().enumerate() {
            let value = read_value(array.column(position).as_ref(), index).map_err(|error| {
                ArrowError::InvalidArgumentError(format!(
                    "struct field \"{}\": {error}",
                    field.name()
                ))
            })?;
            fields.push((field.name().clone(), value));
        }

        Ok(Some(Self { fields }))
    }

    fn array_type() -> DataType {
        DataType::Struct(Fields::empty())
    }
}

impl ToArrow for StructValue {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        let rows: Vec<Option<&StructValue>> = values.iter().map(Option::as_ref).collect();
        struct_array(&rows, field_type)
    }
}

impl FromArrow for ListValue {
    type ArrayType = ListArray;

    fn from_array(array: &Self::ArrayType, index: usize) -> Result<Option<Self>, ArrowError> {
        check_bounds(array.len(), index)?;
        if array.is_null(index) {
            return Ok(None);
        }

        Ok(Some(Self {
            values: read_list(array, index)?,
        }))
    }

    fn array_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Null, true)))
    }
}

impl ToArrow for ListValue {
    fn to_array(values: Vec<Option<Self>>, field_type: &DataType) -> Result<ArrayRef, ArrowError> {
        let DataType::List(inner) = field_type else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "declared output type {field_type} is not a list"
            )));
        };
        let rows: Vec<Option<&Vec<ScalarValue>>> = values
            .iter()
            .map(|value| value.as_ref().map(|list| &list.values))
            .collect();
        list_array(&rows, inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn row_struct() -> StructArray {
        let names = StringArray::from(vec![Some("athena"), None]);
        let sizes = Int64Array::from(vec![Some(3), Some(5)]);
        StructArray::from(vec![
            (
                Arc::new(arrow::datatypes::Field::new("name", DataType::Utf8, true)),
                Arc::new(names) as ArrayRef,
            ),
            (
                Arc::new(arrow::datatypes::Field::new("size", DataType::Int64, true)),
                Arc::new(sizes) as ArrayRef,
            ),
        ])
    }

    fn row_type() -> DataType {
        DataType::Struct(
            vec![
                Arc::new(arrow::datatypes::Field::new("name", DataType::Utf8, true)),
                Arc::new(arrow::datatypes::Field::new("size", DataType::Int64, true)),
            ]
            .into(),
        )
    }

    #[test]
    fn reads_struct_rows() {
        let array = row_struct();

        let first = StructValue::from_array(&array, 0).unwrap().unwrap();
        assert_eq!(
            first.get("name"),
            Some(&ScalarValue::String("athena".to_string()))
        );
        assert_eq!(first.get("size"), Some(&ScalarValue::Int64(3)));

        let second = StructValue::from_array(&array, 1).unwrap().unwrap();
        assert_eq!(second.get("name"), Some(&ScalarValue::Null));
    }

    #[test]
    fn writes_struct_rows() {
        let values = vec![
            Some(StructValue::new([
                ("name", "athena".into()),
                ("size", ScalarValue::Int64(3)),
            ])),
            Some(StructValue::new([
                ("name", ScalarValue::Null),
                ("size", ScalarValue::Int64(5)),
            ])),
        ];

        let array = StructValue::to_array(values, &row_type()).unwrap();
        let array = as_struct_array(array.as_ref());

        assert_eq!(array.len(), 2);
        assert_eq!(
            array
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            5
        );
        assert!(array.column(0).is_null(1));
    }

    #[test]
    fn all_null_rows_use_declared_type() {
        let values: Vec<Option<StructValue>> = vec![None, None];
        let array = StructValue::to_array(values, &row_type()).unwrap();
        let array = as_struct_array(array.as_ref());

        assert_eq!(array.len(), 2);
        assert_eq!(array.num_columns(), 2);
        assert!(array.is_null(0));
        assert!(array.is_null(1));
        assert_eq!(array.column(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn nested_struct_round_trip() {
        let inner_type = row_type();
        let outer_type = DataType::Struct(
            vec![Arc::new(arrow::datatypes::Field::new(
                "child",
                inner_type.clone(),
                true,
            ))]
            .into(),
        );
        let inner = StructValue::new([("name", "athena".into()), ("size", ScalarValue::Int64(3))]);
        let values = vec![Some(StructValue::new([("child", inner.into())]))];

        let array = StructValue::to_array(values, &outer_type).unwrap();
        let outer = as_struct_array(array.as_ref());
        let child = as_struct_array(outer.column(0).as_ref());

        assert_eq!(child.len(), 1);
        assert_eq!(
            child
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "athena"
        );
    }

    #[test]
    fn missing_declared_field_is_an_error() {
        let values = vec![Some(StructValue::new([("name", "athena".into())]))];
        let error = StructValue::to_array(values, &row_type()).unwrap_err();
        assert!(error.to_string().contains("missing the declared field"));
    }

    #[test]
    fn wrong_value_type_is_an_error() {
        let values = vec![Some(StructValue::new([
            ("name", ScalarValue::Int64(1)),
            ("size", ScalarValue::Int64(3)),
        ]))];
        let error = StructValue::to_array(values, &row_type()).unwrap_err();
        assert!(error.to_string().contains("expected a String"));
    }

    #[test]
    fn unsupported_field_type_is_an_error() {
        let names = arrow::array::Date32Array::from(vec![Some(19_000)]);
        let array = StructArray::from(vec![(
            Arc::new(arrow::datatypes::Field::new(
                "created",
                DataType::Date32,
                true,
            )),
            Arc::new(names) as ArrayRef,
        )]);

        let error = StructValue::from_array(&array, 0).unwrap_err();
        assert!(error.to_string().contains("unsupported value type Date32"));
    }

    fn list_of_rows_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", row_type(), true)))
    }

    fn quantities() -> ListValue {
        ListValue::new([
            ScalarValue::Struct(StructValue::new([
                ("name", "athena".into()),
                ("size", ScalarValue::Int64(3)),
            ])),
            ScalarValue::Struct(StructValue::new([
                ("name", "hermes".into()),
                ("size", ScalarValue::Int64(5)),
            ])),
        ])
    }

    #[test]
    fn writes_arrays_of_rows() {
        let values = vec![Some(quantities()), Some(ListValue::default())];
        let array = ListValue::to_array(values, &list_of_rows_type()).unwrap();
        let list = as_list_array(array.as_ref());

        assert_eq!(list.len(), 2);
        assert_eq!(list.value_length(0), 2);
        assert_eq!(list.value_length(1), 0);

        let element = list.value(0);
        let rows = as_struct_array(element.as_ref());
        assert_eq!(
            rows.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            5
        );
    }

    #[test]
    fn reads_arrays_of_rows() {
        let values = vec![Some(quantities())];
        let array = ListValue::to_array(values, &list_of_rows_type()).unwrap();
        let list = as_list_array(array.as_ref());

        let read = ListValue::from_array(list, 0).unwrap().unwrap();
        assert_eq!(read.values().len(), 2);
        let ScalarValue::Struct(first) = &read.values()[0] else {
            panic!("expected a struct element");
        };
        assert_eq!(
            first.get("name"),
            Some(&ScalarValue::String("athena".into()))
        );
    }

    #[test]
    fn null_list_rows_stay_null() {
        let values: Vec<Option<ListValue>> = vec![None, Some(quantities())];
        let array = ListValue::to_array(values, &list_of_rows_type()).unwrap();
        let list = as_list_array(array.as_ref());

        assert!(list.is_null(0));
        assert_eq!(list.value_length(1), 2);
        assert_eq!(ListValue::from_array(list, 0).unwrap(), None);
    }

    #[test]
    fn struct_with_list_field_round_trip() {
        let inner_type = DataType::Struct(
            vec![Arc::new(Field::new("children", list_of_rows_type(), true))].into(),
        );
        let row = StructValue::new([("children", quantities().into())]);

        let array = StructValue::to_array(vec![Some(row)], &inner_type).unwrap();
        let outer = as_struct_array(array.as_ref());
        let read = StructValue::from_array(outer, 0).unwrap().unwrap();
        let Some(ScalarValue::List(children)) = read.get("children") else {
            panic!("expected a list field");
        };
        assert_eq!(children.len(), 2);
    }

    #[test]
    fn case_insensitive_field_lookup_matches_athena_row_fields() {
        let row = StructValue::new([
            ("firstName", "athena".into()),
            ("Size", ScalarValue::Int64(3)),
        ]);
        assert_eq!(
            row.get("firstname"),
            Some(&ScalarValue::String("athena".to_string()))
        );

        let lower_row_type = DataType::Struct(
            vec![
                Arc::new(Field::new("firstname", DataType::Utf8, true)),
                Arc::new(Field::new("size", DataType::Int64, true)),
            ]
            .into(),
        );
        let array = StructValue::to_array(vec![Some(row)], &lower_row_type).unwrap();
        assert_eq!(array.len(), 1);
    }

    #[test]
    fn undeclared_and_duplicate_struct_fields_are_rejected() {
        let extra = StructValue::new([
            ("name", "athena".into()),
            ("size", ScalarValue::Int64(3)),
            ("extra", "ignored".into()),
        ]);
        let error = StructValue::to_array(vec![Some(extra)], &row_type()).unwrap_err();
        assert!(error.to_string().contains("undeclared field \"extra\""));

        let duplicate = StructValue::new([
            ("name", "athena".into()),
            ("name", "hermes".into()),
            ("size", ScalarValue::Int64(3)),
        ]);
        let error = StructValue::to_array(vec![Some(duplicate)], &row_type()).unwrap_err();
        assert!(error.to_string().contains("duplicate field \"name\""));
    }

    #[test]
    fn list_value_collects_and_iterates() {
        let list: ListValue = ["alpha", "beta"].into_iter().collect();
        assert_eq!(list.values().len(), 2);
        assert_eq!((&list).into_iter().count(), 2);
        assert_eq!(list.into_iter().count(), 2);
    }
}
