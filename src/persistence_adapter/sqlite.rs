use std::{sync::Arc, collections::HashMap};
use debug_ignore::DebugIgnore;
use sqlite_::{ConnectionWithFullMutex, Statement};
use sqlite_::State::{Row, Done};
use itertools::intersperse;
use crate::persistence_adapter::{PersistenceAdapter, PersistenceAdapterQueryable, PersistenceSpec, PersistenceType, PersistenceData, StoreError};

use super::Query;

// used for specifying how sqlite should be used to store data
#[derive(Debug, Clone)]
pub struct SqlitePersistence {
    connection: DebugIgnore<Arc<ConnectionWithFullMutex>>,
    table_name: String
}

impl SqlitePersistence{
    pub fn new(connection: Arc<ConnectionWithFullMutex>, table_name: &str) -> Self {
        SqlitePersistence { connection: DebugIgnore(connection), table_name: table_name.to_string() }
    }
}

impl SqlitePersistence {
    fn collect_fields(spec_types: &'static [PersistenceType], prepared_query: &Statement) -> HashMap<String, PersistenceData>{
        let mut data_out = HashMap::new();

        for column in prepared_query.column_names().iter() {
            let column_info = spec_types.iter().filter(|f|f.get_name().eq(column)).next().expect("Unknown table field");
            match column_info {
                PersistenceType::String(n) => {data_out.insert(n.to_string(), PersistenceData::String(prepared_query.read(column.as_str()).expect("Invalid column")));},
                PersistenceType::Bytes(n) => {data_out.insert(n.to_string(), PersistenceData::Bytes(prepared_query.read(column.as_str()).expect("Invalid column")));},
                PersistenceType::Integer(n) => {data_out.insert(n.to_string(), PersistenceData::Integer(prepared_query.read(column.as_str()).expect("Invalid column")));},
                PersistenceType::UnsignedInteger(n) => {data_out.insert(n.to_string(), PersistenceData::UnsignedInteger(prepared_query.read::<i64, &str>(column.as_str()).expect("Invalid column") as u64));},
                PersistenceType::Float(n) =>{data_out.insert(n.to_string(), PersistenceData::Float(prepared_query.read::<f64, &str>(column.as_str()).expect("Invalid column") as f32));},
                PersistenceType::Double(n) => {data_out.insert(n.to_string(), PersistenceData::Double(prepared_query.read(column.as_str()).expect("Invalid column")));},
            }
        }

        data_out
    }

    fn generate_filter(query: &Query, start_index: usize, mut values: Vec<PersistenceData>) -> (String, usize, Vec<PersistenceData>) {
        match query {
            Query::Or(a, b) => {
                let (statement_a, index_end_a, values) = SqlitePersistence::generate_filter(a, start_index, values);
                let (statement_b, index_end_b, values) = SqlitePersistence::generate_filter(b, index_end_a, values);
                (format!("( {} OR {} )", statement_a, statement_b), index_end_b, values)
            },
            Query::And(a, b) => {
                let (statement_a, index_end_a, values) = SqlitePersistence::generate_filter(a, start_index, values);
                let (statement_b, index_end_b, values) = SqlitePersistence::generate_filter(b, index_end_a, values);
                (format!("( {} AND {} )", statement_a, statement_b), index_end_b, values)
            },
            Query::Not(a) => {
                let (statement_a, index_end_a, values) = SqlitePersistence::generate_filter(a, start_index, values);
                (format!("( NOT {} )", statement_a), index_end_a, values)
            },
            Query::Equals(a, b) => {
                values.push(b.clone());
                (format!(" \"{}\"=? ", a), start_index+1, values)
            },
            Query::GreaterThan(a, b) => {
                values.push(b.clone());
                (format!(" \"{}\">? ", a), start_index+1, values)
            },
            Query::LessThan(a, b) => {
                values.push(b.clone());
                (format!(" \"{}\"<? ", a), start_index+1, values)
            },
        }
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
        command.push_str(format!(", PRIMARY KEY ({}) );", Spec::key_field()).as_str());
        println!("{}", command);
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

        prepared_query.next().ok().and_then(|s|{
            match s {
                Row => Spec::deserialize_data(SqlitePersistence::collect_fields(Spec::fields(), &prepared_query)),
                Done => None
            }
        })
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
        command.push_str(" WHERE \"");
        command.push_str(Spec::key_field());
        command.push_str("\"=?");

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

    fn scan(&self, start: usize, limit: Option<usize>) -> Vec<(Key, Data)> {
        let mut command = String::new();
        command.push_str(&format!("SELECT * FROM \"{}\" ORDER BY \"{}\" LIMIT {} OFFSET {}", &self.table_name, Spec::key_field(), limit.map(|l|l as isize).unwrap_or(-1), start));

        let mut prepared_query = self.connection.prepare(command).unwrap();
        let mut rows_out = Vec::new();

        let mut state = prepared_query.next();
        while let Ok(s) = state {
            match s {
                Row => {
                    let fields = SqlitePersistence::collect_fields(Spec::fields(),  &prepared_query);
                    let key = Spec::deserialize_key(fields.get(Spec::key_field()).expect("Key field not present")).expect("Invalid key found while deserializing");
                    match Spec::deserialize_data(fields) {
                        Some(entry) => rows_out.push((key, entry)),
                        None => {}
                    }
                },
                Done => {
                    break;
                }
            }
            state = prepared_query.next();
        }

        rows_out
    }
    
    fn update(&self, key: &Key, data: Data, only_update: Option<&[&str]>) -> Result<(), StoreError> {
        let mut command = String::new();
        command.push_str("UPDATE ");
        command.push_str(&self.table_name.as_str());
        command.push_str(" SET ");
        match only_update{
            Some(k) => intersperse(k.iter().filter(|x|*x!=&Spec::key_field()).map(|name|format!("{} = ?", name)), ", ".to_string()).for_each(|s|command.push_str(&s)),
            None => intersperse(Spec::fields().iter().map(PersistenceType::get_name).filter(|x|x!=&Spec::key_field()).map(|name|format!("{} = ?", name)), ", ".to_string()).for_each(|s|command.push_str(&s)),
        }
        command.push_str(format!(" WHERE {} = :key", Spec::key_field()).as_str());

        println!("Executing {}", command);
        if let Some(serialized) = Spec::serialize_data(&data) {
            let mut statement = self.connection.prepare(command).expect("Invalid statement");
            let _ = match Spec::serialize_key(key) {
                PersistenceData::String(s) => statement.bind((":key", s.as_str())),
                PersistenceData::Bytes(b) => statement.bind((":key", b.as_slice())),
                PersistenceData::Integer(i) => statement.bind((":key", i)),
                PersistenceData::UnsignedInteger(u) => statement.bind((":key", u as i64)),
                PersistenceData::Float(f) => statement.bind((":key", f as f64)),
                PersistenceData::Double(d) => statement.bind((":key", d)),
            };
            let bind_fields = |(field_index, v): (usize, &PersistenceType)|{
                let field_index = field_index + 1;
                let field_name = v.get_name();
                let _ = match serialized.get(field_name).expect("Missing serialized field") {
                    PersistenceData::String(s) => statement.bind((field_index, s.as_str())),
                    PersistenceData::Bytes(b) => statement.bind((field_index, &b[..])),
                    PersistenceData::Integer(i) =>   statement.bind((field_index, *i)),
                    PersistenceData::UnsignedInteger(u) => statement.bind((field_index, *u as i64)),
                    PersistenceData::Float(f) => statement.bind((field_index, *f as f64)),
                    PersistenceData::Double(d) => statement.bind((field_index, *d)),
                };
            };
            match only_update {
                Some(f) => Spec::fields().iter().filter(|v|f.contains(&v.get_name())).enumerate().for_each(bind_fields),
                None => Spec::fields().iter().filter(|v|v.get_name()!=Spec::key_field()).enumerate().for_each(bind_fields),
            }
            let _ = statement.next().map_err(|e|StoreError{message: format!("{e:?}")})?;
            println!("Stored");
            Ok(())
        }else{
            Err(StoreError{ message: "Failed to serialize data".to_string()})
        }
    }
}

impl<Key, Data, Spec: PersistenceSpec<Key, Data>> PersistenceAdapterQueryable<Key, Data, Spec> for SqlitePersistence {
    fn query(&self, query: Query, start: usize, limit: Option<usize>) -> Vec<(Key, Data)> {
        let mut command = String::new();
        let (query_string, _num_placeholders, placeholder_values)  = SqlitePersistence::generate_filter(&query, 0, Vec::new());
        command.push_str(&format!("SELECT * FROM \"{}\" WHERE {} ORDER BY {} LIMIT {} OFFSET {};", &self.table_name, query_string, Spec::key_field(), limit.map(|l|l as isize).unwrap_or(-1), start, ));
        let mut prepared_query = self.connection.prepare(command).unwrap();
        for (i, value) in placeholder_values.iter().enumerate() {
            let bind_field = i+1;
            match value {
                PersistenceData::String(s) => prepared_query.bind((bind_field, s.as_str())),
                PersistenceData::Bytes(b) => prepared_query.bind((bind_field, &b[..])),
                PersistenceData::Integer(i_v) => prepared_query.bind((bind_field, *i_v)),
                PersistenceData::UnsignedInteger(u) => prepared_query.bind((bind_field, *u as i64)),
                PersistenceData::Float(f) => prepared_query.bind((bind_field, *f as f64)),
                PersistenceData::Double(d) => prepared_query.bind((bind_field, *d)),
            }.expect("Failed to bind data");
        }

        let mut rows_out = Vec::new();

        let mut state = prepared_query.next();
        while let Ok(s) = state {
            match s {
                Row => {
                    let fields = SqlitePersistence::collect_fields(Spec::fields(),  &prepared_query);
                    let key = Spec::deserialize_key(fields.get(Spec::key_field()).expect("Key field not present")).expect("Invalid key found while deserializing");
                    match Spec::deserialize_data(fields) {
                        Some(entry) => rows_out.push((key, entry)),
                        None => {}
                    }
                },
                Done => {
                    break;
                }
            }
            state = prepared_query.next();
        }

        rows_out
    }
}
#[cfg(test)]
mod tests{
    use tempdir::TempDir;
    use sqlite_::Connection;
    use std::sync::Arc;
    use rand::{thread_rng, Rng};
    use rand::distributions::Alphanumeric;
    use crate::persistence_adapter::sqlite::SqlitePersistence;
    use crate::tests::AllSupportedTypes;
    use crate::persistence_adapter::{PersistenceAdapter, PersistenceAdapterQueryable, PersistenceData, Query};
    use crate::tests::AllSupportedTypesPersistenceSpec;


    #[tokio::test]
    async fn test_sql_deserialize(){
        let temp_dir = TempDir::new("sqlite_test").expect("Failed to create tempdir");

        let temp_db_name = temp_dir.path().join("test.sqlite");

        let db_connection = Connection::open_with_full_mutex(temp_db_name).expect("Failed to open temp db");

        let persistence = SqlitePersistence::new(Arc::new(db_connection), "test_table");

        PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::initialize(&persistence);

        PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::clear(&persistence);

        assert_eq!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::scan(&persistence, 0, None).len(), 0);

        let x = AllSupportedTypes{
            string: thread_rng().sample_iter(&Alphanumeric).take(64).map(char::from).collect(),
            bytes: thread_rng().gen_iter::<u8>().take(64).collect(),
            integer: thread_rng().gen::<i64>(),
            unsigned_integer: thread_rng().gen::<u32>() as u64,
            float: 0.0,
            double: thread_rng().gen::<f64>()
        };

        let y = AllSupportedTypes{
            string: thread_rng().sample_iter(&Alphanumeric).take(64).map(char::from).collect(),
            bytes: thread_rng().gen_iter::<u8>().take(64).collect(),
            integer: thread_rng().gen::<i64>(),
            unsigned_integer: thread_rng().gen::<u32>() as u64,
            float: 1.0,
            double: thread_rng().gen::<f64>()
        };

        assert!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::store(&persistence, "test".to_string(), x.clone()).is_ok());

        assert!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::contains(&persistence, &("test".to_string())));

        assert_eq!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::load(&persistence, &("test".to_string())), Some(x.clone()));

        assert_eq!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::scan(&persistence, 0, None), vec![("test".to_string(), x.clone())]);

        assert_eq!(PersistenceAdapterQueryable::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::query(&persistence, Query::Equals("key".to_string(), PersistenceData::String("test".to_string())), 0, None), vec![("test".to_string(), x.clone())]);

        assert!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::store(&persistence, "test1".to_string(), y.clone()).is_ok());

        assert_eq!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::scan(&persistence, 0, None), vec![("test".to_string(), x.clone()), ("test1".to_string(), y.clone())]);

        assert_eq!(PersistenceAdapterQueryable::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::query(&persistence, Query::GreaterThan("float".to_string(), PersistenceData::Float(0.0)), 0, None), vec![("test1".to_string(), y.clone())]);

        assert!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::update(&persistence, &"test1".to_string(), x.clone(), Some(&vec!["float"])).is_ok());

        assert_eq!(PersistenceAdapterQueryable::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::query(&persistence, Query::GreaterThan("float".to_string(), PersistenceData::Float(0.0)), 0, None), vec![]);

        assert!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::update(&persistence, &"test1".to_string(), y.clone(), None).is_ok());

        assert!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::delete(&persistence, "test".to_string()).is_some());

        assert_eq!(PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::scan(&persistence, 0, None), vec![("test1".to_string(), y)]);

        assert!(!PersistenceAdapter::<String, AllSupportedTypes, AllSupportedTypesPersistenceSpec>::contains(&persistence, &("test".to_string())));
    }

    #[test]
    fn test_generate_query() {
        let filter = Query::and(
            Query::or(
                Query::not(
                    Query::Equals("integer".to_string(), PersistenceData::Integer(10))
                ),
                Query::Equals("unsigned_integer".to_string(), PersistenceData::UnsignedInteger(10))
            ),
            Query::Equals("string".to_string(), PersistenceData::String("hello!".to_string()))
        );

        println!("{:?}", SqlitePersistence::generate_filter(&filter, 0, Vec::new()));
    }
}