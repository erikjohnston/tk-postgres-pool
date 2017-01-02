use Query;
use futures;

use futures::{Async, BoxFuture, Future, IntoFuture, Poll, Sink, future, task};
use futures::stream::Stream;
use futures::sync::mpsc::{Receiver, Sender, UnboundedSender};

use linear_map::LinearMap;

use pg::FrontendMessage;
use postgres_protocol::authentication::md5_hash as postgres_md5_hash;
use postgres_protocol::message::backend::Message as BackendMessage;
use postgres_protocol::message::backend::ParseResult as BackendParseResult;
use postgres_protocol::message::frontend as pg_frontend;
use std;
use std::collections::VecDeque;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::io::Error as IoError;
use std::mem;
use std::sync::{Arc, Mutex};

use stream_fold::StreamForEach;
use tk_bufstream::{Buf, Decode, Encode, Framed, IoBuf};
use tokio_core;
use tokio_core::io::Io;
use tokio_core::net::TcpStream;


pub struct PostgresCodec;

impl Encode for PostgresCodec {
    type Item = FrontendMessage;

    fn encode(&mut self, item: Self::Item, buf: &mut Buf) {
        let mut vec = Vec::new();
        item.serialize(&mut vec).expect("serialize to work");
        buf.extend(&vec);
    }
}

impl Decode for PostgresCodec {
    type Item = BackendMessage;

    fn decode(&mut self, buf: &mut Buf) -> Result<Option<Self::Item>, IoError> {
        match BackendMessage::parse(buf.as_ref())? {
            BackendParseResult::Complete { message, consumed } => {
                buf.consume(consumed);
                Ok(Some(message))
            }
            BackendParseResult::Incomplete { .. } => Ok(None),
        }
    }
}


pub struct PostgresConnection<T> {
    raw_conn: T,
    queue: VecDeque<Query>,
    resyncing: bool,
    transaction: Option<usize>,
}

impl<T> PostgresConnection<T>
    where T: Sink<SinkItem = FrontendMessage, SinkError = IoError>
{
    pub fn new(conn: T) -> PostgresConnection<T> {
        PostgresConnection {
            raw_conn: conn,
            queue: VecDeque::new(),
            transaction: None,
            resyncing: false,
        }
    }

    pub fn send_query(&mut self, query: Query) -> Result<(), IoError> {
        for msg in &query.data {
            self.raw_conn.start_send(msg.clone())?;
        }
        self.queue.push_back(query);
        Ok(())
    }
}

impl<T> Future for PostgresConnection<T>
    where T: Sink<SinkItem=FrontendMessage, SinkError=IoError>
        + Stream<Item=BackendMessage, Error=IoError>
{
    type Item = ();
    type Error = IoError;

    fn poll(&mut self) -> Poll<(), IoError> {
        loop {
            self.raw_conn.poll_complete()?;
            match self.raw_conn.poll() {
                Ok(Async::Ready(Some(msg))) => {
                    match msg {
                        BackendMessage::PortalSuspended { .. } => {
                            return Err(IoError::new(
                                ErrorKind::Other,
                                "Unexpected Portal Suspended message"
                            ));
                        }
                        BackendMessage::ParameterStatus { .. } => {}
                        msg => {
                            // Work out if the current 'Query' is finished.
                            let is_finished = if let BackendMessage::ReadyForQuery { state } = msg {
                                match state {
                                    b'I' => {}
                                    b'T' => {} // TODO: Ensure we're in txn.
                                    b'E' => {} // TODO
                                    _ => panic!("Unexpected state from PG"), // TODO
                                }

                                true
                            } else {
                                false
                            };

                            // If we're resyncing we wait until the next ReadyForQuery message.
                            if !self.resyncing {
                                if let Some(query) = self.queue.front_mut() {
                                    let sender = &mut query.sender;
                                    if let Err(_) = sender.send(msg) {
                                        // The other side has gone away, mark us as resyncing.
                                        self.resyncing = true;
                                    }
                                } else {
                                    // TODO: Something has probably gone wrong, unless
                                    // its a NoticeMessage or something like that.
                                }
                            }

                            if is_finished {
                                self.resyncing = false;
                                // TODO: Handle if we don't have anything to pop.
                                self.queue.pop_front();
                            }
                        }
                    }
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
                Err(e) => return Err(e), // TODO: Clean up pending queries
            }
        }
    }
}



pub trait ConnectionFactory<T> {
    type Future: Future<Item = T, Error = IoError>;

    fn connect(&mut self) -> Self::Future;
}


pub struct TcpConnectionFactory {
    addr: std::net::SocketAddr,
    handle: tokio_core::reactor::Handle,
}

impl ConnectionFactory<tokio_core::net::TcpStream> for TcpConnectionFactory {
    type Future = tokio_core::net::TcpStreamNew;

    fn connect(&mut self) -> tokio_core::net::TcpStreamNew {
        tokio_core::net::TcpStream::connect(&self.addr, &self.handle)
    }
}


pub struct PostgresConnectionFactory<T, CF: ConnectionFactory<T>> {
    conn_fac: CF,
    username: String,
    password: String,
    _data: std::marker::PhantomData<T>,
}

impl<T: Io, CF: ConnectionFactory<T>> PostgresConnectionFactory<T, CF> {
    pub fn connect(&mut self)
        -> impl Future<Item = PostgresConnection<Framed<T, PostgresCodec>>,
                  Error = IoError> {

        let auth_params = AuthParams {
            username: self.username.clone(),
            password: self.password.clone(),
        };

        self.conn_fac
            .connect()
            .and_then(move |conn| {
                let pg_conn = IoBuf::new(conn).framed(PostgresCodec);

                auth_params.start(pg_conn)
            })
    }
}



pub struct AuthParams {
    username: String,
    password: String,
}

impl AuthParams {
    pub fn start<T>(self, conn: T)
        -> impl Future<Item = PostgresConnection<T>, Error = IoError>
        where T: Sink<SinkItem=FrontendMessage, SinkError=IoError>
            + Stream<Item=BackendMessage, Error=IoError>
    {
        let startup = FrontendMessage::StartupMessage {
            parameters: vec![("user".into(), self.username.clone())],
        };
        conn.send(startup)
            .and_then(move |conn| {
                StreamForEach::new(conn, move |msg, conn: T| {
                    // We lift this function up here so that we don't to
                    // copy it.
                    let send_password = move |password: String, conn: T| {
                        let m = FrontendMessage::PasswordMessage {
                            password: password,
                        };
                        let f = conn.send(m);
                        future::Either::B(f.map(|conn| (false, conn)))
                    };

                    use BackendMessage::*;
                    match msg {
                        AuthenticationMD5Password { salt } => {
                            let md5ed =
                                postgres_md5_hash(self.username.as_bytes(),
                                                  self.password.as_bytes(),
                                                  salt);

                            send_password(md5ed, conn)
                        }
                        AuthenticationCleartextPassword => {
                            send_password(self.password.clone(), conn)
                        }
                        AuthenticationOk => {
                            future::Either::A(Ok((true, conn)).into_future())
                        }
                        AuthenticationGSS |
                        AuthenticationKerberosV5 |
                        AuthenticationSCMCredential |
                        AuthenticationSSPI => {
                            let err = IoError::new(ErrorKind::Other,
                                                   "Unsupported \
                                                    authentication type \
                                                    requested");
                            future::Either::A(Err(err).into_future())
                        }
                        ErrorResponse { fields } => {
                            let map: LinearMap<_, _> = fields.into_iter()
                                .collect();
                            let err = IoError::new(ErrorKind::Other,
                                                   map[&b'M'].to_owned());
                            future::Either::A(Err(err).into_future())
                        }
                        _ => {
                            let err = IoError::new(ErrorKind::Other,
                                                   "Unexpected message from \
                                                    backend");
                            future::Either::A(Err(err).into_future())
                        }
                    }
                })
            })
            .and_then(move |conn| {
                if let Some(conn) = conn {
                    Ok(PostgresConnection::new(conn))
                } else {
                    Err(IoError::new(ErrorKind::UnexpectedEof, ""))
                }
            })
    }
}


#[cfg(test)]
mod tests {
    use futures::StartSend;
    use futures::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
    use std::collections::BTreeMap;
    use super::*;


    struct TestConnBacking {
        pub output: UnboundedSender<BackendMessage>,
        pub input: UnboundedReceiver<FrontendMessage>,
    }

    impl Stream for TestConnBacking {
        type Item = FrontendMessage;
        type Error = IoError;

        fn poll(&mut self) -> Poll<Option<Self::Item>, IoError> {
            self.input
                .poll()
                .map_err(|_| panic!("Connection has dropped"))
        }
    }

    impl TestConnBacking {
        fn send_msg(&mut self, msg: BackendMessage) {
            if let Err(_) = self.output.start_send(msg) {
                println!("Failed to send msg, conn dropped.");
            }
        }
    }

    fn create_conn() -> (TestConn, TestConnBacking) {
        let (input_sender, input_rc) = unbounded();
        let (output_sender, output_rc) = unbounded();

        let conn = TestConn {
            output: output_rc,
            input: input_sender,
        };

        let backing = TestConnBacking {
            output: output_sender,
            input: input_rc,
        };

        (conn, backing)
    }

    struct TestConn {
        pub output: UnboundedReceiver<BackendMessage>,
        pub input: UnboundedSender<FrontendMessage>,
    }

    impl Sink for TestConn {
        type SinkItem = FrontendMessage;
        type SinkError = IoError;

        fn start_send(
            &mut self,
            item: Self::SinkItem
        ) -> StartSend<Self::SinkItem, Self::SinkError> {
            self.input
                .start_send(item)
                .map_err(|_| panic!(""))
        }

        fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
            self.input
                .poll_complete()
                .map_err(|_| panic!(""))
        }
    }

    impl Stream for TestConn {
        type Item = BackendMessage;
        type Error = IoError;

        fn poll(&mut self) -> Poll<Option<Self::Item>, IoError> {
            self.output
                .poll()
                .map_err(|_| panic!(""))
        }
    }

    #[test]
    fn test_startup_no_auth() {
        futures::lazy(|| {
            let auth_params = AuthParams {
                username: "foo".into(),
                password: "bar".into(),
            };

            let (conn, backing) = create_conn();
            let client_startup_future = auth_params.start(conn);

            let server_future = backing.into_future()
                .and_then(|(item, mut backing)| {
                    match item {
                        Some(FrontendMessage::StartupMessage { parameters }) => {
                            let parameters: BTreeMap<String, String> = parameters.into_iter().collect();
                            assert_eq!(parameters["user"], "foo");
                            println!("Got starup message");
                        }
                        _ => panic!("Expected startup message"),
                    }
                    backing.send_msg(BackendMessage::AuthenticationOk);
                    Ok(())
                })
                .map_err(|(err, _)| err);

            server_future.join(client_startup_future)
        }).wait().unwrap();
    }

    #[test]
    fn test_startup_cleartext_auth() {
        futures::lazy(|| {
            let auth_params = AuthParams {
                username: "foo".into(),
                password: "bar".into(),
            };

            let (conn, backing) = create_conn();
            let client_startup_future = auth_params.start(conn);

            let server_future = backing.into_future()
                .and_then(|(item, mut backing)| {
                    match item {
                        Some(FrontendMessage::StartupMessage { .. }) => {
                            println!("Got starup message");
                        }
                        _ => panic!("Expected startup message"),
                    }
                    backing.send_msg(BackendMessage::AuthenticationCleartextPassword);
                    backing.into_future()
                })
                .and_then(|(item, mut backing)| {
                    match item {
                        Some(FrontendMessage::PasswordMessage { password }) => {
                            assert_eq!(&password, "bar");
                            println!("Got password message");
                        }
                        _ => panic!("Expected password message"),
                    }
                    backing.send_msg(BackendMessage::AuthenticationOk);
                    Ok(())
                })
                .map_err(|(err, _)| err);

            server_future.join(client_startup_future)
        }).wait().unwrap();
    }
}