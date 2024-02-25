#[cfg(feature = "sqlite")]
use std::{sync::Arc, collections::HashMap};
use debug_ignore::DebugIgnore;
use sqlite_::ConnectionWithFullMutex;
use sqlite_::State::{Row, Done};
use itertools::intersperse;
use crate::{persistence_adapter::{PersistenceAdapter, PersistenceSpec, PersistenceType, PersistenceData, StoreError}};

// used for specifying how sqlite should be used to store data
#[derive(Debug, Clone)]
pub struct SqlitePersistence {
    connection: DebugIgnore<Arc<ConnectionWithFullMutex>>,
    table_name: String
}

impl SqlitePersistence {
    pub fn new(connection: Arc<ConnectionWithFullMutex>, table_name: &str) -> Self {
        SqlitePersistence { connection: DebugIgnore(connection), table_name: table_name.to_string() }
    }
}

impl<Key, Data, Spec: PersistenceSpec<Key, Data>> PersistenceAdapter<Key, Data, Spec> for SqlitePersistence {
    fn initialize(&self) -> Option<()> {
        let mut command = String::new();
        command.push_str("CREATE TABLE IF NOT EXISTS \"");
        command.push_str(&self.table_name);
        command.push_str("\" (");
        intersperse(Spec::fields().iter().map(|e|{
            match e {
                PersistenceType::String(name) => format!("{name} TEXT"),
                PersistenceType::Bytes(name) => format!("{name} BLOB"),
                PersistenceType::Integer(name) |  PersistenceType::UnsignedInteger(name) => format!("{name} INTEGER"),
                PersistenceType::Float(name)   |  PersistenceType::Double(name) => format!("{name} REAL"),
            }
        }), ", ".to_string()).for_each(|s|command.push_str(&s));
        command.push_str(");");
        self.connection.execute(command).ok()
    }

    fn load(&self, key: &Key) -> Option<Data> {
        let mut command = String::new();
        command.push_str("SELECT * FROM \"");
        command.push_str(&self.table_name);
        command.push_str("\" WHERE \"");
        command.push_str(Spec::key_field());
        command.push_str("\" = :primary_key");

        let mut prepared_query = self.connection.prepare(command).unwrap();

        let serialized_key = Spec::serialize_key(key);


        match serialized_key {
            PersistenceData::String(s) => {prepared_query.bind((":primary_key", s.as_str())).ok()?},
            PersistenceData::Bytes(b) => {prepared_query.bind((":primary_key", &b[..])).ok()?},
            PersistenceData::Integer(i) => {prepared_query.bind((":primary_key", i)).ok()?},
            PersistenceData::UnsignedInteger(u) => {prepared_query.bind((":primary_key", u as i64)).ok()?},
            PersistenceData::Float(f) => {prepared_query.bind((":primary_key", f as f64)).ok()?},
            PersistenceData::Double(d) => {prepared_query.bind((":primary_key", d)).ok()?},
        };

        let mut data_out = HashMap::new();
        let mut state = prepared_query.next();
        while let Ok(s) = state {
            match s {
                Row => {
                    for column in prepared_query.column_names().iter() {
                        let column_info = Spec::fields().iter().filter(|f|f.get_name().eq(column)).next().expect("Unknown table field");
                        match column_info {
                            PersistenceType::String(n) => {data_out.insert(n.to_string(), PersistenceData::String(prepared_query.read(column.as_str()).expect("Invalid column")));},
                            PersistenceType::Bytes(n) => {data_out.insert(n.to_string(), PersistenceData::Bytes(prepared_query.read(column.as_str()).expect("Invalid column")));},
                            PersistenceType::Integer(n) => {data_out.insert(n.to_string(), PersistenceData::Integer(prepared_query.read(column.as_str()).expect("Invalid column")));},
                            PersistenceType::UnsignedInteger(n) => {data_out.insert(n.to_string(), PersistenceData::UnsignedInteger(prepared_query.read::<i64, &str>(column.as_str()).expect("Invalid column") as u64));},
                            PersistenceType::Float(n) =>{data_out.insert(n.to_string(), PersistenceData::Float(prepared_query.read::<f64, &str>(column.as_str()).expect("Invalid column") as f32));},
                            PersistenceType::Double(n) => {data_out.insert(n.to_string(), PersistenceData::Double(prepared_query.read(column.as_str()).expect("Invalid column")));},
                        }
                    }
                }
                Done => break,
            }
            state = prepared_query.next();
        }
        Spec::deserialize_data(data_out)
    }

    fn store(&self, key: Key, data: Data) -> Result<(), crate::persistence_adapter::StoreError> {
        let mut command = String::new();
        command.push_str("INSERT INTO ");
        command.push_str(&self.table_name.as_str());
        command.push_str(" (");
        intersperse(Spec::fields().iter().map(PersistenceType::get_name), ", ").for_each(|s|command.push_str(s));

        command.push_str(") values (");
        
        intersperse(Spec::fields().iter().map(|_|"?"), ", ").for_each(|s|command.push_str(s));

        command.push_str(")");

        if let Some(serialized) = Spec::serialize_data(&data) {
            let mut statement = self.connection.prepare(command).expect("Invalid statement");
            let serialized_key = Spec::serialize_key(&key);
            Spec::fields().iter().enumerate().for_each(|(field_index, v)|{
                let field_index = field_index + 1;
                let field_name = v.get_name();
                let _ = match serialized.get(field_name).or_else(||if field_name == Spec::key_field() {Some(&serialized_key)}else{None}).expect("Missing serialized field") {
                    PersistenceData::String(s) => statement.bind((field_index, s.as_str())),
                    PersistenceData::Bytes(b) => statement.bind((field_index, &b[..])),
                    PersistenceData::Integer(i) =>   statement.bind((field_index, *i)),
                    PersistenceData::UnsignedInteger(u) => statement.bind((field_index, *u as i64)),
                    PersistenceData::Float(f) => statement.bind((field_index, *f as f64)),
                    PersistenceData::Double(d) => statement.bind((field_index, *d)),
                };
            });
            let _ = statement.next().map_err(|e|StoreError{message: format!("{e:?}")})?;
            println!("Stored");
            Ok(())
        }else{
            Err(StoreError{ message: "Failed to serialize data".to_string()})
        }
        
    }

    fn delete(&self, key: Key) -> Option<()> {
        let mut command = String::new();

        command.push_str("DELETE FROM ");
        command.push_str(&self.table_name);
        command.push_str("WHERE ");
        command.push_str(Spec::key_field());
        command.push_str("=?");

        let mut statement = self.connection.prepare(command).expect("Invalid command");
        let _ = match Spec::serialize_key(&key) {
            PersistenceData::String(s) => statement.bind((1, s.as_str())),
            PersistenceData::Bytes(b) => statement.bind((1, &b[..])),
            PersistenceData::Integer(i) => statement.bind((1, i)),
            PersistenceData::UnsignedInteger(u) => statement.bind((1, u as i64)),
            PersistenceData::Float(f) => statement.bind((1, f as f64)),
            PersistenceData::Double(d) => statement.bind((1, d)),
        };
        println!("Deleted");
        let _ = statement.next().ok()?;

        Some(())
    }

    fn contains(&self, key: &Key) -> bool {
        let mut command = String::new();

        command.push_str("SELECT ");
        command.push_str(Spec::key_field());
        command.push_str(" FROM ");
        command.push_str(&self.table_name);
        command.push_str(" WHERE ");
        command.push_str(Spec::key_field());
        command.push_str("=?");

        println!("contains: {command}");
        let mut statement = self.connection.prepare(command).expect("Invalid command");
        let _ = match Spec::serialize_key(&key) {
            PersistenceData::String(s) => statement.bind((1, s.as_str())),
            PersistenceData::Bytes(b) => statement.bind((1, &b[..])),
            PersistenceData::Integer(i) => statement.bind((1, i)),
            PersistenceData::UnsignedInteger(u) => statement.bind((1, u as i64)),
            PersistenceData::Float(f) => statement.bind((1, f as f64)),
            PersistenceData::Double(d) => statement.bind((1, d)),
        };

        'read_lines: while let Ok(s) = statement.next() {
            match s {
                sqlite_::State::Row => return true,
                _ => break 'read_lines
            }
        }
        println!("Contains");
        return false;
    }

    fn clear(&self) {
        println!("All rows deleted from {}", self.table_name);
        let mut command = String::new();
        command.push_str("DELETE FROM ");
        command.push_str(&self.table_name);
        let _ = self.connection.execute(command);
        println!("Clear");
    }
}

