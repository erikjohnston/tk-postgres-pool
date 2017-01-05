use Query;
use futures::{Async, Poll, Stream};
use futures::sync::mpsc::UnboundedSender;
use linear_map::LinearMap;
use pg::FrontendMessage;
use postgres_protocol;
use postgres_protocol::message::backend::Message as BackendMessage;
use std::io::Error as IoError;
use std::io::ErrorKind;
use stream_fold::StreamFold;
use types::{FromSql, ToSql};
use types::Type;
use vec_stream::{VecStreamReceiver, create_stream};
use std::ops::Deref;
use std::vec::IntoIter;
use std::slice::Iter;


#[derive(Clone)]
pub struct Client {
    query_sender: UnboundedSender<Query>,
}

impl Client {
    pub fn new(query_sender: UnboundedSender<Query>) -> Client {
        Client { query_sender: query_sender }
    }

    pub fn execute<S: Into<String>>(&mut self, query: S, params: &[&SerializeSql]) -> RowsStream {
        let mut formats = Vec::with_capacity(params.len());
        let mut values = Vec::with_capacity(params.len());

        for param in params {
            let value = param.serialize().map(|s| s.into_bytes());
            formats.push(0);
            values.push(value);
        }

        let msg = vec![FrontendMessage::Parse {
                           name: String::new(),
                           query: query.into(),
                           param_types: vec![0; params.len()],
                       },
                       FrontendMessage::Bind {
                           portal: String::new(),
                           statement: String::new(),
                           formats: formats,
                           values: values,
                           result_formats: vec![1],
                       },
                       FrontendMessage::Describe {
                           variant: b'P',
                           name: String::new(),
                       },
                       FrontendMessage::Execute {
                           portal: String::new(),
                           max_rows: 0,
                       }];

        let (sender, receiver) = create_stream();

        let query = Query {
            data: msg,
            sender: sender,
        };

        self.query_sender.send(query).expect("ConnectionPool was dropped");

        RowsStream {
            receiver: receiver,
            column_types: Vec::new(),
        }
    }
}

pub trait SerializeSql {
    fn serialize(&self) -> Option<String>;
}


impl SerializeSql for String {
    fn serialize(&self) -> Option<String> {
        Some(self.clone())
    }
}

impl<'a> SerializeSql for &'a str {
    fn serialize(&self) -> Option<String> {
        Some((*self).to_owned())
    }
}

impl SerializeSql for i8 {
    fn serialize(&self) -> Option<String> {
        let string = format!("{}", self);
        Some(string)
    }
}

impl SerializeSql for i16 {
    fn serialize(&self) -> Option<String> {
        let string = format!("{}", self);
        Some(string)
    }
}

impl SerializeSql for i32 {
    fn serialize(&self) -> Option<String> {
        let string = format!("{}", self);
        Some(string)
    }
}

impl SerializeSql for i64 {
    fn serialize(&self) -> Option<String> {
        let string = format!("{}", self);
        Some(string)
    }
}

impl<'a, T: SerializeSql> SerializeSql for &'a [T] {
    fn serialize(&self) -> Option<String> {
        let vec: Vec<_> = self.iter()
            .map(|t| t.serialize().expect("Non null values in array"))
            .map(|mut string| {
                string.insert(0, '"');
                string.push('"');
                string
            })
            .collect();

        let string = vec.join(",");

        let string = format!("{{{}}}", string);
        Some(string)
    }
}


pub struct RowsStream {
    receiver: VecStreamReceiver<BackendMessage, IoError>,
    column_types: Vec<Type>,
}

impl Stream for RowsStream {
    type Item = Row;
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let msg = match self.receiver.poll()? {
                Async::NotReady => return Ok(Async::NotReady),
                Async::Ready(None) => return Ok(Async::Ready(None)),
                Async::Ready(Some(msg)) => msg,
            };

            match msg {
                BackendMessage::RowDescription { descriptions } => {
                    for entry in descriptions {
                        self.column_types
                            .push(Type::from_oid(entry.type_oid).expect("Valid type OID"));
                    }
                }
                BackendMessage::DataRow { row } => {
                    let vec = row.into_iter()
                        .zip(&self.column_types)
                        .map(|(column, ctype)| {
                            Column {
                                raw: column,
                                ctype: ctype.clone(),
                            }
                        })
                        .collect();

                    return Ok(Async::Ready(Some(Row {
                        cols: vec,
                    })));
                }
                BackendMessage::ErrorResponse { fields } => {
                    let map: LinearMap<_, _> = fields.into_iter().collect();
                    let err = IoError::new(ErrorKind::Other, map[&b'M'].to_owned());
                    return Err(err);
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    cols: Vec<Column>,
}

impl Row {
    pub fn get<T: FromSql>(&self, idx: usize) -> T {
        self.cols[idx].get()
    }
}

impl Deref for Row {
    type Target = [Column];

    fn deref(&self) -> &[Column] {
        &self.cols[..]
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a Column;
    type IntoIter = Iter<'a, Column>;

    fn into_iter(self) -> Iter<'a, Column> {
        (&self.cols).into_iter()
    }
}


impl IntoIterator for Row {
    type Item = Column;
    type IntoIter = IntoIter<Column>;

    fn into_iter(self) -> IntoIter<Column> {
        self.cols.into_iter()
    }
}


#[derive(Debug, Clone)]
pub struct Column {
    raw: Option<Vec<u8>>,
    ctype: Type,
}

impl Column {
    pub fn get<T: FromSql>(&self) -> T {
        let bytes_opt = self.raw.as_ref().map(|v| &v[..]);
        T::from_sql_nullable(&self.ctype, bytes_opt)
            .expect("could not convert column to requested type")
    }
}
