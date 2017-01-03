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
mod vec_stream;


use connection::{ConnectionFactory, PostgresConnection, IoPostgresConnectionFactory};
use vec_stream::VecStreamSender;


pub struct Query {
    data: Vec<FrontendMessage>,
    sender: VecStreamSender<BackendMessage, ()>,
    retryable: bool,
}


pub struct ConnectionPool<C: ConnectionFactory> {
    connections: Vec<C::Item>,
    new_connections: Vec<C::Future>,
    queue_receiver: Option<Receiver<Query>>,
    query_queue: VecDeque<Query>,
    connection_factory: C,
}

impl<C> ConnectionPool<C> where C: ConnectionFactory, C::Item: PostgresConnection
{
    fn poll_new_connections(&mut self) -> bool {
        let mut changed = false;

        let new_vec = Vec::with_capacity(self.new_connections.len());
        let new_conns = mem::replace(&mut self.new_connections, new_vec);
        for mut future in new_conns {
            match future.poll() {
                Ok(Async::Ready(conn)) => {
                    self.connections.push(conn);
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
                self.new_query(query);
                true
            }
            Ok(Async::NotReady) => false,
            Ok(Async::Ready(None)) | Err(()) => return false,
        };

        self.queue_receiver = Some(queue_receiver);

        changed
    }

    fn poll_open_connetions(&mut self) -> bool {
        let new_vec = Vec::with_capacity(self.connections.len());
        let conns = mem::replace(&mut self.connections, new_vec);
        for mut conn in conns {
            match conn.poll() {
                Ok(Async::NotReady) => self.connections.push(conn),
                _ => {}
            }
        }

        false
    }

    fn new_query(&mut self, query: Query) {
        if !self.query_queue.is_empty() {
            self.query_queue.push_back(query);
            return;
        }

        self.query_queue.shrink_to_fit(); // Since we know its empty.

        self.connections.sort_by_key(|conn| !conn.pending_queries());
        // TODO: Make '5' configurable.
        let pos_opt = self.connections.iter().position(|conn| conn.pending_queries() < 5);
        if let Some(pos) = pos_opt {
            let mut conn = self.connections.swap_remove(pos);
            match conn.send_query(query) {
                Ok(()) => self.connections.push(conn),
                Err(_) => {} // TODO: Report error.
            }
        } else {
            // TODO: Make '5' configurable.
            if self.new_connections.is_empty() && self.connections.len() < 5 {
                let fut = self.connection_factory.connect();
                self.new_connections.push(fut);
            }
            self.query_queue.push_back(query);
        }
    }
}

impl<C> Future for ConnectionPool<C>
    where C: ConnectionFactory, C::Item: PostgresConnection
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        while {
            self.poll_receiver()
            || self.poll_new_connections()
            || self.poll_open_connetions()
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
