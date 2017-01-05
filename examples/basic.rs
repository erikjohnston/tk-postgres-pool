extern crate tokio_postgres;
extern crate tokio_core;
extern crate futures;
extern crate postgres_protocol;
extern crate env_logger;


use futures::Stream;
use postgres_protocol::message::backend::Message;
use tokio_core::net::TcpStream;
use tokio_postgres::{ConnectionFactory, ConnectionPool, FrontendMessage,
                     IoPostgresConnectionFactory, Query, TcpConnectionFactory,
                     backend_message_type};


fn main() {
    env_logger::init().unwrap();

    let args: Vec<_> = std::env::args().collect();
    let password = args.get(1).expect("expected password to be supplied");

    let mut core = tokio_core::reactor::Core::new().unwrap();

    let conn_fac = TcpConnectionFactory::new("127.0.0.1:5432".parse().unwrap(), core.handle());
    let pg_fac = IoPostgresConnectionFactory::new(conn_fac, "erikj", &password[..]);

    let mut client = ConnectionPool::spawn(pg_fac, core.handle());

    core.run(client.execute("SELECT * FROM test WHERE test = $1", &[&"foo\n"])
            .for_each(|row| {
                println!("Received msg: {:?}", row);

                for column in row {
                    println!("Column as _: {:?}", column.get::<String>());
                }

                Ok(())
            }))
        .unwrap();
}
