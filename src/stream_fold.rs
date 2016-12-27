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

use futures::{Async, Future, Poll};
use futures::stream::Stream;

use std::mem;


/// A Stream adapater, similar to fold, that consumes the start of
/// the stream to build up an object, but then returns both the object
/// *and* the stream.
#[must_use = "futures do nothing unless polled"]
pub struct StreamForEach<I, E, S: Stream<Item = I, Error = E>, F> {
    func: F,
    state: StreamForEachState<S>,
}

impl<I, E, S: Stream<Item = I, Error = E>, F> StreamForEach<I, E, S, F> {
    pub fn new(stream: S, func: F) -> StreamForEach<I, E, S, F> {
        StreamForEach {
            func: func,
            state: StreamForEachState::Full { stream: stream },
        }
    }
}

enum StreamForEachState<S> {
    Empty,
    Full { stream: S },
}

impl<I, E, S: Stream<Item = I, Error = E>, F> Future
    for StreamForEach<I, E, S, F>
    where F: FnMut(I, &mut S) -> Result<bool, E>
{
    type Item = Option<S>;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, E> {
        let mut stream = match mem::replace(&mut self.state,
                                            StreamForEachState::Empty) {
            StreamForEachState::Empty => panic!("cannot poll Fold twice"),
            StreamForEachState::Full { stream } => stream,
        };

        loop {
            match stream.poll()? {
                Async::Ready(Some(item)) => {
                    let done = (self.func)(item, &mut stream)?;
                    if done {
                        return Ok(Async::Ready(Some(stream)));
                    }
                }
                Async::Ready(None) => return Ok(Async::Ready(None)),
                Async::NotReady => {
                    self.state = StreamForEachState::Full { stream: stream };
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}
