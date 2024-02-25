

pub mod persistence_adapter {
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
        pub fn to_bytes(self) -> Option<Vec<u8>> {
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
    }
}

