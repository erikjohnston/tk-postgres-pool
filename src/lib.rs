#![feature(conservative_impl_trait)]
#![allow(unused_imports, dead_code)]

#[macro_use]
extern crate futures;
#[macro_use]
extern crate tokio_core;
extern crate postgres_protocol;
extern crate tk_bufstream;
extern crate linear_map;
#[macro_use]
extern crate log;
extern crate fallible_iterator;


use futures::{Async, BoxFuture, Future, IntoFuture, Poll, Sink, future, task};
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};

pub use pg::{FrontendMessage, backend_message_type};
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


mod client;
mod pg;
mod stream_for_each;
mod stream_fold;
mod connection;
pub mod vec_stream;
mod types;


pub use client::{Client, SerializeSql};
pub use connection::{ConnectionFactory, IoPostgresConnectionFactory, PostgresConnection,
                     TcpConnectionFactory};
use vec_stream::VecStreamSender;


pub struct Query {
    pub data: Vec<FrontendMessage>,
    pub sender: VecStreamSender<BackendMessage, IoError>,
}


pub struct ConnectionPool<C: ConnectionFactory> {
    connections: Vec<C::Item>,
    new_connections: Vec<C::Future>,
    queue_receiver: Option<UnboundedReceiver<Query>>,
    query_queue: VecDeque<Query>,
    connection_factory: C,
}

impl<C> ConnectionPool<C>
    where C: ConnectionFactory + 'static,
          C::Item: PostgresConnection
{
    pub fn spawn(connection_factory: C, handle: tokio_core::reactor::Handle) -> Client {
        let (sender, receiver) = futures::sync::mpsc::unbounded();
        let pool = ConnectionPool {
            connections: Vec::new(),
            new_connections: Vec::new(),
            queue_receiver: Some(receiver),
            query_queue: VecDeque::new(),
            connection_factory: connection_factory,
        };

        handle.spawn(pool);

        Client::new(sender)
    }

    fn poll_new_connections(&mut self) -> bool {
        debug!("poll_new_connections");

        let mut changed = false;

        let new_vec = Vec::with_capacity(self.new_connections.len());
        let new_conns = mem::replace(&mut self.new_connections, new_vec);
        for mut future in new_conns {
            match future.poll() {
                Ok(Async::Ready(conn)) => {
                    self.connections.push(conn);
                    changed = true;
                    self.handle_pending_queries();
                }
                Ok(Async::NotReady) => self.new_connections.push(future),
                Err(err) => {
                    // TODO: Report error...
                    panic!("Failed to connect!");
                }
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
                debug!("Handling new query");
                if let Some(query) = self.new_query(query) {
                    debug!("Queuing new query");
                    self.query_queue.push_back(query);
                }
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

    fn new_query(&mut self, query: Query) -> Option<Query> {
        self.connections.sort_by_key(|conn| !conn.pending_queries());
        // TODO: Make '5' configurable.
        let pos_opt = self.connections.iter().position(|conn| conn.pending_queries() < 5);
        if let Some(pos) = pos_opt {
            let mut conn = self.connections.swap_remove(pos);

            if conn.pending_queries() > 1 && self.connections.len() + self.new_connections.len() < 5 {
                let fut = self.connection_factory.connect();
                self.new_connections.push(fut);
            }

            match conn.send_query(query) {
                Ok(()) => self.connections.push(conn),
                Err(_) => {} // TODO: Report error.
            }
            None
        } else {
            // TODO: Make '5' configurable.
            if self.new_connections.is_empty() && self.connections.len() < 5 {
                let fut = self.connection_factory.connect();
                self.new_connections.push(fut);
            }
            Some(query)
        }
    }

    fn handle_pending_queries(&mut self) {
        debug!("Handling pending queries");
        while let Some(query) = self.query_queue.pop_front() {
            if let Some(query) = self.new_query(query) {
                self.query_queue.push_front(query);
                return;
            }
        }
    }
}

impl<C> Future for ConnectionPool<C>
    where C: ConnectionFactory + 'static,
          C::Item: PostgresConnection
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<()>, ()> {
        while {
            self.poll_receiver() || self.poll_new_connections() || self.poll_open_connetions()
        } {}

        let finished = self.queue_receiver.is_none() && self.connections.is_empty() &&
                       self.new_connections.is_empty();

        if finished {
            // TODO: Reject all pending queries?
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}
