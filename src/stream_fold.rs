use futures::{Async, Future, IntoFuture, Poll};
use futures::stream::Stream;

use std::mem;


#[must_use = "futures do nothing unless polled"]
pub struct StreamForEach<S, F, U>
    where U: IntoFuture
{
    func: F,
    state: State<S, U>,
}

impl<S, F, U> StreamForEach<S, F, U>
    where U: IntoFuture
{
    pub fn new(stream: S, func: F) -> StreamForEach<S, F, U> {
        StreamForEach {
            func: func,
            state: State::Stream(stream),
        }
    }
}

enum State<S, U>
    where U: IntoFuture
{
    Empty,
    Future(U::Future),
    Stream(S),
}

impl<I, E, S, F, U> Future for StreamForEach<S, F, U>
    where S: Stream<Item = I, Error = E>,
          F: FnMut(I, S) -> U,
          U: IntoFuture<Item = (bool, S), Error = E>
{
    type Item = Option<S>;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, E> {
        let state = mem::replace(&mut self.state, State::Empty);

        let mut future = match state {
            State::Empty => panic!("cannot poll StreamForEach twice"),
            State::Stream(mut stream) => {
                let item = match stream.poll()? {
                    Async::Ready(Some(item)) => item,
                    Async::Ready(None) => return Ok(Async::Ready(None)),
                    Async::NotReady => {
                        self.state = State::Stream(stream);
                        return Ok(Async::NotReady);
                    }
                };

                (self.func)(item, stream).into_future()
            }
            State::Future(future) => future,
        };

        match future.poll() {
            Ok(Async::Ready((done, stream))) => {
                if done {
                    Ok(Async::Ready(Some(stream)))
                } else {
                    self.state = State::Stream(stream);
                    Ok(Async::NotReady)
                }
            }
            Ok(Async::NotReady) => {
                self.state = State::Future(future);
                Ok(Async::NotReady)
            }
            Err(e) => {
                self.state = State::Empty;
                Err(e)
            }
        }
    }
}
