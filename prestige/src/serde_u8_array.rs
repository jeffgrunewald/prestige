/// Serde helper for `[u8; N]` fields that serializes as a sequence (not a tuple).
///
/// serde's default `[T; N]` implementation uses `serialize_tuple`/`deserialize_tuple`,
/// which serde_arrow doesn't support for Arrow list columns. This module uses the
/// `serialize_seq`/`deserialize_seq` protocol instead, enabling round-trip through
/// `List(UInt8)` schemas.
///
/// Auto-injected by `#[prestige_schema]` on `[u8; N]` fields without `#[prestige(as_binary)]`.
use serde::{
    Deserializer, Serializer,
    de::{SeqAccess, Visitor},
};

pub fn serialize<const N: usize, S: Serializer>(
    array: &[u8; N],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(N))?;
    for byte in array {
        seq.serialize_element(byte)?;
    }
    seq.end()
}

pub fn deserialize<'de, const N: usize, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<[u8; N], D::Error> {
    struct ArrayVisitor<const N: usize>;

    impl<'de, const N: usize> Visitor<'de> for ArrayVisitor<N> {
        type Value = [u8; N];

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a sequence of {N} bytes")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut arr = [0u8; N];
            for (i, byte) in arr.iter_mut().enumerate() {
                *byte = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
            }
            Ok(arr)
        }
    }

    deserializer.deserialize_seq(ArrayVisitor::<N>)
}

/// Serde helper for `Option<[u8; N]>` fields using the sequence protocol.
pub mod option {
    use serde::{Deserializer, Serializer};

    pub fn serialize<const N: usize, S: Serializer>(
        value: &Option<[u8; N]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(array) => super::serialize(array, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, const N: usize, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<[u8; N]>, D::Error> {
        use serde::de::Visitor;

        struct OptionArrayVisitor<const N: usize>;

        impl<'de, const N: usize> Visitor<'de> for OptionArrayVisitor<N> {
            type Value = Option<[u8; N]>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "an optional sequence of {N} bytes")
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }

            fn visit_some<D: Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<Self::Value, D::Error> {
                super::deserialize(deserializer).map(Some)
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
        }

        deserializer.deserialize_option(OptionArrayVisitor::<N>)
    }
}
