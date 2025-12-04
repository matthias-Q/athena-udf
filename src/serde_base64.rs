use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Deserializer, Serializer};

/// Serializes a byte slice to a base64-encoded string for serde.
///
/// This function is intended to be used with serde's `#[serde(with = "...")]` attribute
/// to automatically encode binary data as base64 when serializing to formats like JSON.
///
/// # Arguments
///
/// * `bytes` - The byte slice to encode
/// * `serializer` - The serde serializer
///
/// # Examples
///
/// ```
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStruct {
///     #[serde(with = "athena_udf::serde_base64")]
///     data: Vec<u8>,
/// }
/// ```
pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&general_purpose::STANDARD.encode(bytes))
}

/// Deserializes a base64-encoded string to a byte vector for serde.
///
/// This function is intended to be used with serde's `#[serde(with = "...")]` attribute
/// to automatically decode base64 strings to binary data when deserializing from formats like JSON.
///
/// # Arguments
///
/// * `deserializer` - The serde deserializer
///
/// # Errors
///
/// Returns an error if the string is not valid base64.
///
/// # Examples
///
/// ```
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct MyStruct {
///     #[serde(with = "athena_udf::serde_base64")]
///     data: Vec<u8>,
/// }
/// ```
pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    general_purpose::STANDARD
        .decode(s)
        .map_err(serde::de::Error::custom)
}
