

pub mod persistence_adapter {
    #[cfg(feature = "sqlite")]
    pub mod sqlite;

    use std::{collections::HashMap, fmt::Display};

    // Used for specifying data and how it should be stored
    #[allow(dead_code)]
    pub enum PersistenceType{
        String(&'static str),
        Bytes(&'static str),
        Integer(&'static str),
        UnsignedInteger(&'static str),
        Float(&'static str),
        Double(&'static str)
    }

    impl PersistenceType {
        pub fn get_name(&self) -> &'static str {
            match self {
                PersistenceType::String(n) => n,
                PersistenceType::Bytes(n) => n,
                PersistenceType::Integer(n) => n,
                PersistenceType::UnsignedInteger(n) => n,
                PersistenceType::Float(n) => n,
                PersistenceType::Double(n) => n,
            }
        }
    }

    #[derive(Debug)]
    pub enum PersistenceData{
        String(String),
        Bytes(Vec<u8>),
        Integer(i64),
        UnsignedInteger(u64),
        Float(f32),
        Double(f64)
    }


    impl<'a> PersistenceData {
        #[allow(dead_code)]
        pub fn to_bytes(&self) -> Option<&[u8]> {
            if let PersistenceData::Bytes(b) = self {
                return Some(b)
            }

            None
        }

        pub fn to_unsigned_int(&self) -> Option<u64> {
            if let PersistenceData::UnsignedInteger(u) = self {
                return Some(*u)
            }

            None
        }

        pub fn to_str(&'a self) -> Option<&'a str> {
            if let PersistenceData::String(s) = self {
                return Some(s)
            }
            None
        }

        pub fn to_int(&self) -> Option<i64> {
            if let PersistenceData::Integer(i) = self {
                return Some(*i)
            }
            None
        }

        pub fn to_float(&self) -> Option<f32> {
            if let PersistenceData::Float(f) = self {
                return Some(*f)
            }
            None
        }

        pub fn to_double(&self) -> Option<f64> {
            if let PersistenceData::Double(d) = self {
                return Some(*d)
            }
            None
        }
    }

    #[derive(Debug)]
    pub struct StoreError {
        pub message: String
    }

    impl Display for StoreError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl std::error::Error for StoreError {}

    // How data should be represented when stored
    pub trait PersistenceSpec<Key, Data>{
        fn fields()-> &'static [PersistenceType]; // all fields that should be present, including the primary key
        fn key_field() -> &'static str;
        fn serialize_key(key: &Key) -> PersistenceData;
        fn serialize_data(data: &Data) -> Option<HashMap<&'static str, PersistenceData>>;
        fn deserialize_data(data: HashMap<String, PersistenceData>) -> Option<Data>;
    }

    // How to store and retrieve data

    pub trait PersistenceAdapter<Key, Data, Spec: PersistenceSpec<Key, Data>> {
        fn initialize(&self) -> Option<()>;
        fn load(&self, key: &Key) -> Option<Data>;
        fn delete(&self, key: Key) -> Option<()>;
        fn store(&self, key: Key, data: Data) -> Result<(), StoreError>;
        fn contains(&self, key: &Key) -> bool;
        fn clear(&self);
        fn scan(&self, start: usize, limit: Option<usize>) -> Vec<Data>;
    }
}

#[cfg(test)]
pub(crate) mod tests{
    use std::collections::HashMap;

    use crate::persistence_adapter::{PersistenceData, PersistenceSpec, PersistenceType};

    #[derive(Clone, PartialEq, Debug)]
    pub(crate) struct AllSupportedTypes {
        pub(crate) string: String,
        pub(crate) bytes: Vec<u8>,
        pub(crate) integer: i64,
        pub(crate) unsigned_integer: u64,
        pub(crate) float: f32,
        pub(crate) double: f64
    }

    const TEST_FIELDS: [PersistenceType; 7] = [
        PersistenceType::String("key"),
        PersistenceType::String("string"),
        PersistenceType::Bytes("bytes"),
        PersistenceType::Integer("integer"),
        PersistenceType::UnsignedInteger("unsigned_integer"),
        PersistenceType::Float("float"),
        PersistenceType::Double("double")
    ];

    pub(crate) struct AllSupportedTypesPersistenceSpec {}

    impl PersistenceSpec<String, AllSupportedTypes> for AllSupportedTypesPersistenceSpec {
        fn fields()-> &'static [crate::persistence_adapter::PersistenceType] {
            &TEST_FIELDS
        }

        fn key_field() -> &'static str {
            "key"
        }

        fn serialize_key(key: &String) -> crate::persistence_adapter::PersistenceData {
            PersistenceData::String(key.clone())
        }

        fn serialize_data(data: &AllSupportedTypes) -> Option<std::collections::HashMap<&'static str, crate::persistence_adapter::PersistenceData>> {
            Some(HashMap::from(
                [
                    ("string", PersistenceData::String(data.string.clone())),
                    ("bytes", PersistenceData::Bytes(data.bytes.clone())),
                    ("integer", PersistenceData::Integer(data.integer)),
                    ("unsigned_integer", PersistenceData::UnsignedInteger(data.unsigned_integer)),
                    ("float", PersistenceData::Float(data.float)),
                    ("double", PersistenceData::Double(data.double))
                ]
            ))
        }

        fn deserialize_data(data: std::collections::HashMap<String, crate::persistence_adapter::PersistenceData>) -> Option<AllSupportedTypes> {
            Some(
                AllSupportedTypes{
                    string: data.get("string").and_then(PersistenceData::to_str)?.to_string(),
                    bytes: data.get("bytes").and_then(PersistenceData::to_bytes)?.to_vec(),
                    integer: data.get("integer").and_then(PersistenceData::to_int)?,
                    unsigned_integer: data.get("unsigned_integer").and_then(PersistenceData::to_unsigned_int)?,
                    float: data.get("float").and_then(PersistenceData::to_float)?,
                    double: data.get("double").and_then(PersistenceData::to_double)?,
                }
            )
        }
    }
}