use Query;
use futures::{Stream, Poll, Async};
use futures::sync::mpsc::UnboundedSender;
use pg::FrontendMessage;
use types::Type;
use postgres_protocol::message::backend::Message as BackendMessage;
use types::{FromSql, ToSql};
use vec_stream::{create_stream, VecStreamReceiver};
use stream_fold::StreamFold;
use std::io::Error as IoError;
use std::io::ErrorKind;
use postgres_protocol;
use linear_map::LinearMap;


#[derive(Clone)]
pub struct Client {
    query_sender: UnboundedSender<Query>,
}

impl Client {
    pub fn new(query_sender: UnboundedSender<Query>) -> Client {
        Client {
            query_sender: query_sender
        }
    }

    pub fn execute<S: Into<String>>(&mut self, query: S, params: &[&SerializeSql]) -> Rows {
        let mut formats = Vec::with_capacity(params.len());
        let mut values = Vec::with_capacity(params.len());

        for param in params {
            let (format, value) = param.serialize();
            formats.push(format);
            values.push(value);
        }

        let msg = vec![
            FrontendMessage::Parse {
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
            },
        ];

        let (sender, receiver) = create_stream();

        let query = Query {
            data: msg,
            sender: sender,
        };

        self.query_sender.send(query).expect("ConnectionPool was dropped");

        Rows {
            receiver: receiver,
            column_types: Vec::new(),
        }
    }
}

pub trait SerializeSql {
    fn serialize(&self) -> (i16, Option<Vec<u8>>);
}


impl SerializeSql for String {
    fn serialize(&self) -> (i16, Option<Vec<u8>>){
        (0, Some(self.as_bytes().to_owned()))
    }
}

impl<'a> SerializeSql for &'a str {
    fn serialize(&self) -> (i16, Option<Vec<u8>>){
        (0, Some(self.as_bytes().to_owned()))
    }
}

impl SerializeSql for i8 {
    fn serialize(&self) -> (i16, Option<Vec<u8>>) {
        let string = format!("{}", self);
        (0, Some(string.into_bytes()))
    }
}

impl SerializeSql for i16 {
    fn serialize(&self) -> (i16, Option<Vec<u8>>) {
        let string = format!("{}", self);
        (0, Some(string.into_bytes()))
    }
}

impl SerializeSql for i32 {
    fn serialize(&self) -> (i16, Option<Vec<u8>>) {
        let string = format!("{}", self);
        (0, Some(string.into_bytes()))
    }
}

impl SerializeSql for i64 {
    fn serialize(&self) -> (i16, Option<Vec<u8>>) {
        let string = format!("{}", self);
        (0, Some(string.into_bytes()))
    }
}


pub struct Rows {
    receiver: VecStreamReceiver<BackendMessage, IoError>,
    column_types: Vec<Type>,
}

impl Stream for Rows {
    type Item = Vec<Column>;
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
                        self.column_types.push(Type::from_oid(entry.type_oid).expect("Valid type OID"));
                    }
                }
                BackendMessage::DataRow { row } => {
                    let vec = row.into_iter().zip(&self.column_types).map(|(column, ctype)| {
                        Column {
                            raw: column,
                            ctype: ctype.clone(),
                        }
                    }).collect();

                    return Ok(Async::Ready(Some(vec)));
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
pub struct Column {
    raw: Option<Vec<u8>>,
    ctype: Type,
}

impl Column {
    pub fn get<T: FromSql>(&self) -> T {
        let bytes_opt = self.raw.as_ref().map(|v| &v[..]);
        T::from_sql_nullable(&self.ctype, bytes_opt).expect("could not convert column to requested type")
    }
}