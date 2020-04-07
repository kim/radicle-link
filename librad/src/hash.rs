// This file is part of radicle-link
// <https://github.com/radicle-dev/radicle-link>
//
// Copyright (C) 2019-2020 The Radicle Team <dev@radicle.xyz>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 3 or
// later as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use multihash::{Blake2b256, Multihash};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// A hash function, suitable for small inputs
pub trait Hasher: PartialEq + Eq {
    /// Hash the supplied slice
    fn hash(data: &[u8]) -> Self;
}

#[derive(Debug, Error)]
#[error("Invalid hash algorithm, expected {expected:?}, actual {actual:?}")]
pub struct AlgorithmMismatch {
    expected: multihash::Code,
    actual: multihash::Code,
}

/// A hash obtained using the default hash function
///
/// Use this type for all hashing needs which don't depend on VCS specifics.
/// Currently, this uses Blake2b-256 for compatibility with `radicle-registry`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hash(Multihash);

impl Hasher for Hash {
    fn hash(data: &[u8]) -> Self {
        Hash(Blake2b256::digest(data))
    }
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Nb. `deserialize_bytes` is NOT the inverse of `serialize_bytes`. Instead, we
        // need to go through an owned sequence here.
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let mhash = Multihash::from_bytes(bytes).map_err(serde::de::Error::custom)?;
        match mhash.algorithm() {
            multihash::Code::Blake2b256 => Ok(Self(mhash)),
            c => Err(serde::de::Error::custom(AlgorithmMismatch {
                expected: multihash::Code::Blake2b256,
                actual: c,
            })),
        }
    }
}

#[cfg(test)]
mod fast {
    use std::hash::Hasher;

    use fnv::FnvHasher;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// A fast, but not cryptographically secure hash function
    ///
    /// **Only** use this in test code which does not rely on collision
    /// resistance properties of the hash function.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct FastHash(u64);

    impl super::Hasher for FastHash {
        fn hash(data: &[u8]) -> Self {
            let mut hasher = FnvHasher::default();
            hasher.write(data);
            Self(hasher.finish())
        }
    }

    impl Serialize for FastHash {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_u64(self.0)
        }
    }

    impl<'de> Deserialize<'de> for FastHash {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            u64::deserialize(deserializer).map(Self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{fast::*, *};

    use std::fmt::Debug;

    use rand::random;

    fn is_a_deterministic_function<H: Hasher + Debug>() {
        let data: [u8; 32] = random();
        assert_eq!(H::hash(&data), H::hash(&data))
    }

    fn can_serde<H>()
    where
        for<'de> H: Hasher + Debug + Serialize + Deserialize<'de>,
    {
        let data: [u8; 32] = random();
        let hash = H::hash(&data);

        let json = serde_json::to_string(&hash).unwrap();
        let de1 = serde_json::from_str(&json).unwrap();

        let cbor = serde_cbor::to_vec(&hash).unwrap();
        let de2 = serde_cbor::from_slice(&cbor).unwrap();

        assert_eq!(de1, de2);
        assert_eq!(hash, de1);
    }

    #[test]
    fn test_determinism() {
        is_a_deterministic_function::<Hash>();
        is_a_deterministic_function::<FastHash>();
    }

    #[test]
    fn test_serde() {
        can_serde::<Hash>();
        can_serde::<FastHash>();
    }

    #[test]
    fn test_serde_wrong_algorithm() {
        let data: [u8; 32] = random();

        let sha3 = multihash::Sha3_256::digest(&data);

        let json = serde_json::to_string(&sha3.as_bytes()).unwrap();
        let de: Result<Hash, serde_json::Error> = serde_json::from_str(&json);

        // Bravo, serde: the std::error::Error impls only return a `source()` for IO
        // errors. So no option but to match against the `Display` impl. Sorry, future
        // maintainer!
        let expect_err = de.unwrap_err().to_string();
        assert_eq!(
            &expect_err,
            "Invalid hash algorithm, expected Blake2b256, actual Sha3_256"
        )
    }
}