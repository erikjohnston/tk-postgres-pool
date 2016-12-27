#![feature(conservative_impl_trait)]
#![allow(unused_imports, dead_code)]

#[macro_use]
extern crate futures;
#[macro_use]
extern crate tokio_core;
extern crate postgres_protocol;
extern crate tk_bufstream;


use futures::{Async, BoxFuture, Future, Poll, task};
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender, UnboundedSender};
use postgres_protocol::authentication::md5_hash as postgres_md5_hash;
use postgres_protocol::message::backend::Message as BackendMessage;
use postgres_protocol::message::backend::ParseResult as BackendParseResult;
use postgres_protocol::message::frontend as pg_frontend;
use postgres_protocol::message::frontend::Message as FrontendMessage;
use std::collections::VecDeque;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::io::Error as IoError;
use std::mem;
use std::sync::{Arc, Mutex};
use tk_bufstream::{Buf, Decode, Encode, Framed, IoBuf};
use tokio_core::io::Io;
use tokio_core::net::TcpStream;


mod stream_fold;


struct RawPostgresConnection<T: Io> {
    stream: IoBuf<T>,
    temp_vec: Vec<u8>,
}

impl<T: Io> RawPostgresConnection<T> {
    fn new(conn: T) -> RawPostgresConnection<T> {
        RawPostgresConnection {
            stream: IoBuf::new(conn),
            temp_vec: Vec::new(),
        }
    }

    fn write(&mut self, buf: &[u8]) {
        self.stream.out_buf.extend(buf);
    }

    fn flush(&mut self) -> Result<(), IoError> {
        self.stream.flush()
    }
}

impl<T: Io> Stream for RawPostgresConnection<T> {
    type Item = BackendMessage;
    type Error = IoError;

    fn poll(&mut self) -> Result<Async<Option<BackendMessage>>, IoError> {
        loop {
            match BackendMessage::parse(&self.stream.in_buf[..])? {
                BackendParseResult::Complete { message, consumed } => {
                    self.stream.in_buf.consume(consumed);
                    return Ok(Async::Ready(Some(message)));
                }
                BackendParseResult::Incomplete { .. } => {
                    if self.stream.read()? == 0 {
                        if self.stream.done() {
                            return Ok(Async::Ready(None));
                        } else {
                            return Ok(Async::NotReady);
                        }
                    }
                }
            }
        }
    }
}

struct PostgresConnection<T: Io> {
    raw_conn: RawPostgresConnection<T>,
    queue: VecDeque<Query>,
    transaction: Option<usize>,
}

impl<T: Io> PostgresConnection<T> {
    fn new(conn: RawPostgresConnection<T>) -> PostgresConnection<T> {
        PostgresConnection {
            raw_conn: conn,
            queue: VecDeque::new(),
            transaction: None,
        }
    }

    fn handle_poll(&mut self) {
        loop {
            match self.raw_conn.poll() {
                Ok(Async::Ready(Some(msg))) => {
                    match msg {
                        BackendMessage::ReadyForQuery { state } => {
                            match state {
                                b'I' => {}
                                b'T' => {} // TODO: Ensure we're in txn.
                                b'E' => {} // TODO
                                _ => panic!("Unexpected state from PG"), // TODO
                            }
                            self.queue.pop_front();
                        }
                        // TODO: Handle PortalSuspended.
                        msg => {
                            // TODO: Some things are not in response to
                            // anything.
                            // TODO: Check we have a sender...
                            let res = self.queue[0].sender.send(msg);
                            if let Err(err) = res {
                                // TODO: Need to stop sending to this
                            }
                        }
                    }
                }
                Ok(Async::NotReady) => return,
                Ok(Async::Ready(None)) |
                Err(_) => return, // TODO
            }
        }
    }

    fn send_query(&mut self, query: Query) -> Result<(), IoError> {
        self.raw_conn.write(&query.data);
        self.raw_conn.flush()?;
        self.queue.push_back(query);
        Ok(())
    }

    fn accepting(&self) -> bool {
        // TODO: Handle slow queries
        // TODO: Make configurable
        self.queue.len() < 5 && self.transaction.is_none()
    }
}


struct Query {
    data: Vec<u8>,
    sender: UnboundedSender<BackendMessage>,
    retryable: bool,
}


pub struct ConnectionPool<T: Io> {
    connections: VecDeque<PostgresConnection<T>>,
    new_connections: Vec<Box<Future<Item = PostgresConnection<T>,
                                    Error = IoError>>>,
    queue_receiver: Option<Receiver<Query>>,
}

impl<T: Io> ConnectionPool<T> {
    fn poll_new_connections(&mut self) -> bool {
        let mut changed = false;

        let new_vec = Vec::with_capacity(self.new_connections.len());
        let new_conns = mem::replace(&mut self.new_connections, new_vec);
        for mut future in new_conns {
            match future.poll() {
                Ok(Async::Ready(conn)) => {
                    self.connections.push_back(conn);
                    changed = true;
                }
                Ok(Async::NotReady) => self.new_connections.push(future),
                Err(err) => {} // TODO: Report error...
            }
        }

        changed
    }

    fn poll_receiver(&mut self) -> bool {
        let mut queue_receiver = if let Some(q) = self.queue_receiver.take() {
            q
        } else {
            return false;
        };

        let changed = match queue_receiver.poll() {
            Ok(Async::Ready(Some(query))) => {
                // TODO: Stuff
                let mut conn = self.connections.pop_front().unwrap();
                if let Ok(()) = conn.send_query(query) {
                    self.connections.push_back(conn);
                }
                true
            }
            Ok(Async::NotReady) => false,
            Ok(Async::Ready(None)) | Err(()) => return false,
        };

        self.queue_receiver = Some(queue_receiver);

        changed
    }
}

impl<T: Io> Future for ConnectionPool<T> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        while {
            self.poll_receiver() || self.poll_new_connections()
        } {}

        let finished = self.queue_receiver.is_none() &&
                       self.connections.is_empty() &&
                       self.new_connections.is_empty();

        if finished {
            // TODO: Reject all pending queries?
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}


trait ConnectionFactory<T> {
    type Future: Future<Item = T, Error = IoError>;

    fn connect(&mut self) -> Self::Future;
}


struct TcpConnectionFactory {
    addr: std::net::SocketAddr,
    handle: tokio_core::reactor::Handle,
}

impl ConnectionFactory<tokio_core::net::TcpStream> for TcpConnectionFactory {
    type Future = tokio_core::net::TcpStreamNew;

    fn connect(&mut self) -> tokio_core::net::TcpStreamNew {
        tokio_core::net::TcpStream::connect(&self.addr, &self.handle)
    }
}


struct PostgresConnectionFactory<T: Io, CF: ConnectionFactory<T>> {
    conn_fac: CF,
    username: String,
    password: String,
    _data: std::marker::PhantomData<T>,
}

impl<T: Io, CF: ConnectionFactory<T>> PostgresConnectionFactory<T, CF> {
    fn connect
        (&mut self)
        -> impl Future<Item = PostgresConnection<T>, Error = IoError> {
        let username = self.username.clone();
        let password = self.password.clone();

        self.conn_fac
            .connect()
            .and_then(move |conn| {
                let mut pg_conn = RawPostgresConnection::new(conn);
                let mut buf = Vec::new();
                pg_frontend::startup_message(None, &mut buf)?;
                pg_conn.write(&buf);
                pg_conn.flush()?;

                Ok(pg_conn)
            })
            .and_then(move |pg_conn| {
                stream_fold::StreamForEach::new(
                    pg_conn,
                    move |msg, conn: &mut RawPostgresConnection<T>| {
                        use BackendMessage::*;
                        match msg {
                            AuthenticationMD5Password { salt } => {
                                let md5ed = postgres_md5_hash(
                                    username.as_bytes(),
                                    password.as_bytes(),
                                    salt,
                                );

                                let mut buf = Vec::new();
                                pg_frontend::password_message(
                                    &md5ed,
                                    &mut buf,
                                )?;
                                conn.write(&buf);
                                conn.flush()?;

                                Ok(false)
                            }
                            AuthenticationOk => Ok(true),
                            _ => panic!(""), // TODO
                        }
                    }
                )
            })
            .and_then(move |pg_conn| {
                if let Some(conn) = pg_conn {
                    Ok(PostgresConnection::new(conn))
                } else {
                    Err(IoError::new(ErrorKind::UnexpectedEof, ""))
                }
            })
    }
}
