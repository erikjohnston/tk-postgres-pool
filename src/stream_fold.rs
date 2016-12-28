// Copyright 2016 Openmarket
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::{Async, Future, IntoFuture, Poll};
use futures::stream::Stream;

use std::mem;


/// A Stream adapater, similar to fold, that consumes the start of
/// the stream to build up an object, but then returns both the object
/// *and* the stream.
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
                    Async::NotReady => return Ok(Async::NotReady),
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
