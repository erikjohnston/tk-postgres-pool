#![feature(conservative_impl_trait)]
#![allow(unused_imports, dead_code)]

#[macro_use]
extern crate futures;
#[macro_use]
extern crate tokio_core;
extern crate postgres_protocol;
extern crate tk_bufstream;
extern crate linear_map;


use futures::{Async, BoxFuture, Future, IntoFuture, Poll, Sink, future, task};
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender, UnboundedSender};

use pg::FrontendMessage;
use postgres_protocol::authentication::md5_hash as postgres_md5_hash;
use postgres_protocol::message::backend::Message as BackendMessage;
use postgres_protocol::message::backend::ParseResult as BackendParseResult;
use postgres_protocol::message::frontend as pg_frontend;
use std::collections::VecDeque;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::io::Error as IoError;
use std::mem;
use std::sync::{Arc, Mutex};
use tk_bufstream::{Buf, Decode, Encode, Framed, IoBuf};
use tokio_core::io::Io;
use tokio_core::net::TcpStream;


mod pg;
mod stream_fold;
mod connection;


use connection::PostgresConnection;


pub struct Query {
    data: Vec<FrontendMessage>,
    sender: UnboundedSender<BackendMessage>,
    retryable: bool,
}


pub struct ConnectionPool<T> {
    connections: VecDeque<PostgresConnection<T>>,
    new_connections: Vec<Box<Future<Item = PostgresConnection<T>,
                                    Error = IoError>>>,
    queue_receiver: Option<Receiver<Query>>,
}

impl<T> ConnectionPool<T>
    where T: Sink<SinkItem=FrontendMessage, SinkError=IoError>
        + Stream<Item=BackendMessage, Error=IoError>
{
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
// TODO: Launch new connections
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

impl<T: Io> Future for ConnectionPool<T>
    where T: Sink<SinkItem=FrontendMessage, SinkError=IoError>
        + Stream<Item=BackendMessage, Error=IoError>
{
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
