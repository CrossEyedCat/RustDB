//! Bincode helpers using the **legacy** wire format (compatible with bincode 1.x on-disk data).
//!
//! Используется [`bincode_next`](https://crates.io/crates/bincode-next) (форк bincode) с
//! [`bincode_next::config::legacy()`] для serde-сериализации.

use bincode_next::config::legacy;

/// Serialize with bincode 1–compatible wire format.
pub fn serialize<T: serde::Serialize>(
    value: &T,
) -> Result<Vec<u8>, bincode_next::error::EncodeError> {
    bincode_next::serde::encode_to_vec(value, legacy())
}

/// Deserialize with bincode 1–compatible wire format.
pub fn deserialize<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, bincode_next::error::DecodeError> {
    let (value, _) = bincode_next::serde::decode_from_slice(bytes, legacy())?;
    Ok(value)
}

/// Deserialize from a reader (e.g. [`std::io::Cursor`] over `&[u8]`).
pub fn deserialize_from_reader<T: serde::de::DeserializeOwned, R: std::io::Read>(
    reader: &mut R,
) -> Result<T, bincode_next::error::DecodeError> {
    bincode_next::serde::decode_from_std_read(reader, legacy())
}
