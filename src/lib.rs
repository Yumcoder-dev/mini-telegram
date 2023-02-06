// Copyright 2022 mini-telegram Project Authors. Licensed under Apache-2.0.

//! A minimal (i.e. very incomplete) implementation of a [MTProto](https://core.telegram.org/mtproto) (telegram) server.
//!
//! The purpose of this project is to provide an
//! asynchronous Rust MTProto project built with Tokio. Do not attempt to run this in
//! production... seriously.
//!
//! The major components are:
//!
//! * `server`: MTProto(telegram) server implementation.
//!
//! * `client`: an asynchronous MTProto client implementation.
//!
//! * `frame`: represents a single MTProto protocol frame. A frame is used as an
//!   intermediate representation between a "command" and the byte
//!   representation.

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
