//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

// see https://redis.io/topics/protocol
// - For Simple Strings the first byte of the reply is "+"
// - For Errors the first byte of the reply is "-"
// - For Integers the first byte of the reply is ":"
// - For Bulk Strings the first byte of the reply is "$"
// - For Arrays the first byte of the reply is "*"
//
// RESP (REdis Serialization Protocol)
// The way RESP is used in Redis as a request-response protocol is the following:
// - Clients send commands to a Redis server as a RESP Array of Bulk Strings.
// - The server replies with one of the RESP types according to the command implementation.

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// A frame in the Redis protocol.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    // Clients send commands to the Redis server using RESP Array
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push a "bulk" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Push an "integer" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?; // "-Error message\r\n"
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?; // ":1000\r\n"
                Ok(())
            }
            b'$' => {
                // Bulk Strings are encoded in the following way:
                // - A "$" byte followed by the number of bytes composing the
                //   string (a prefixed length), terminated by CRLF.
                // - The actual string data.
                // - A final CRLF.
                if b'-' == peek_u8(src)? {
                    // Bulk Strings can also be used in order to signal non-existence
                    // of a value using a special format that is used to represent a Null value.
                    // In this special format the length is -1, and there is no data,
                    // so a Null is represented as: "$-1\r\n"

                    // Skip '-1\r\n'
                    skip(src, 4)
                } else {
                    // Read the bulk string
                    let len: usize = get_decimal(src)?.try_into()?; // need to impl From<TryFromIntError>

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, len + 2)
                }
            }
            b'*' => {
                // Arrays are sent using the following format:
                // - A "*" character as the first byte, followed by the number of
                //   elements in the array as a decimal number, followed by CRLF.

                let len = get_decimal(src)?; // get array length
                                             // An array with 5 elements:
                                             // *5\r\n
                                             // :1\r\n
                                             // :2\r\n
                                             // :3\r\n
                                             // :4\r\n
                                             // $6\r\n
                                             // foobar\r\n
                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()), // impl From<&str>
        }
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?; // impl From<FromUtf8Error>

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // Read the line and convert it to `Vec<u8>`
                let line = get_line(src)?.to_vec();

                // Convert the line to a String
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // skip that number of bytes + 2 (\r\n).
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// Converts the frame to an "unexpected frame" error
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame: {}", self).into() // impl fmt::Display
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(fmt, " ")?;
                    }
                    part.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Find a line
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Scan the bytes directly
    let start = src.position() as usize;
    // Scan to the second to last byte
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // We found a line, update the position to be *after* the \n
            src.set_position((i + 2) as u64);

            // Return the line
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cursor() {
        use std::io::prelude::*;
        use std::io::SeekFrom;

        let mut buff = Cursor::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(buff.position(), 0);
        buff.seek(SeekFrom::Current(2)).unwrap();
        assert_eq!(buff.position(), 2);

        buff.seek(SeekFrom::Current(-1)).unwrap();
        assert_eq!(buff.position(), 1);

        buff.set_position(2);
        assert_eq!(buff.position(), 2);
        assert_eq!(buff.remaining_slice(), &[3, 4, 5]);

        buff.set_position(10);
        assert!(buff.is_empty());

        buff.set_position(2);
        assert!(buff.has_remaining());

        buff.advance(1);
        assert_eq!(buff.position(), 3);
        assert!(buff.has_remaining());

        assert_eq!(buff.get_ref().len(), 5);
    }

    #[test]
    fn test_cursor_get_line() {
        let stream = &b"+10\r\n+20\r\n"[..];
        let mut buff = Cursor::new(stream);

        let res10 = get_line(&mut buff).unwrap();
        assert_eq!(String::from_utf8(res10.to_vec()).unwrap(), "+10");

        let res20 = get_line(&mut buff).unwrap();
        assert_eq!(String::from_utf8(res20.to_vec()).unwrap(), "+20");
    }

    #[test]
    fn test_make_frame() {
        let mut get_frame = Frame::array();
        get_frame.push_bulk(Bytes::from("get"));
        get_frame.push_bulk(Bytes::from("key"));

        assert_eq!(get_frame.to_string(), "get key");
        // println!("{:?}", get_frame); // Array([Bulk(b"get"), Bulk(b"key")])

        let mut set_frame = Frame::array();
        set_frame.push_bulk(Bytes::from("set"));
        set_frame.push_bulk(Bytes::from("key"));
        set_frame.push_int(100);
        assert_eq!(set_frame.to_string(), "set key 100");
    }

    #[test]
    fn test_frame_check() {
        let tests = vec![
            (&b"+OK\r\n"[..], Ok(())), // (stream, expected result)
            (
                &b"#\r\n"[..],
                Err(Error::Other(
                    "protocol error; invalid frame type byte `35`".into(),
                )),
            ),
            (&b""[..], Err(Error::Incomplete)),
            (&b"-Error message\r\n"[..], Ok(())), // error message
            (&b":1000\r\n"[..], Ok(())),          //  an integer
            (&b"$6\r\nfoobar\r\n"[..], Ok(())),   // string "foobar"
            (&b"$0\r\n\r\n"[..], Ok(())),         // empty string
            (&b"$-1\r\n"[..], Ok(())),            // null string
            (&b"*0\r\n"[..], Ok(())),             // an empty Array
            (
                // an array of two Bulk Strings "foo" and "bar"
                &b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..],
                Ok(()),
            ),
            (
                //  an Array of three integers
                &b"*3\r\n:1\r\n:2\r\n:3\r\n"[..],
                Ok(()),
            ),
            (
                // a list of four integers and a bulk string
                &b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"[..],
                Ok(()),
            ),
            (
                //  an Array containing a Null element ["foo",nil,"bar"]
                &b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"[..],
                Ok(()),
            ),
        ];

        for t in tests.iter() {
            let stream = t.0;
            let wanted = &t.1;
            let mut buff = Cursor::new(stream);
            let res = Frame::check(&mut buff);

            match res {
                Ok(_) => assert!(wanted.is_ok()),
                Err(e) => assert_eq!(wanted.as_ref().unwrap_err().to_string(), e.to_string()),
            }
        }
    }

    #[test]
    fn test_frame_parse() {
        // for easer comperation uses string as expected result
        let tests = vec![
            (&b"+OK\r\n"[..], Ok("OK")), // (stream, expected result)
            (&b""[..], Err(Error::Incomplete)),
            (
                // error message
                &b"-Error message\r\n"[..],
                Ok("error: Error message"),
            ),
            (&b":1000\r\n"[..], Ok("1000")), //  an integer
            (
                // string "foobar"
                &b"$6\r\nfoobar\r\n"[..],
                Ok("foobar"),
            ),
            (
                // empty string
                &b"$0\r\n\r\n"[..],
                Ok(""),
            ),
            (&b"$-1\r\n"[..], Ok("(nil)")), // null string
            (
                // an empty Array
                &b"*0\r\n"[..],
                Ok(""),
            ),
            (
                // an array of two Bulk Strings "foo" and "bar"
                &b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..],
                Ok("foo bar"),
            ),
            (
                //  an Array of three integers
                &b"*3\r\n:1\r\n:2\r\n:3\r\n"[..],
                Ok("1 2 3"),
            ),
            (
                // a list of four integers and a bulk string
                &b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"[..],
                Ok("1 2 3 4 foobar"),
            ),
            (
                //  an Array containing a Null element ["foo",nil,"bar"]
                &b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"[..],
                Ok("foo (nil) bar"),
            ),
        ];

        for t in tests.iter() {
            let stream = t.0;
            let wanted = &t.1;
            let mut buff = Cursor::new(stream);
            let res = Frame::parse(&mut buff);
            match res {
                Ok(x) => {
                    assert_eq!(x.to_string(), wanted.as_ref().unwrap().to_string());
                }
                Err(e) => assert_eq!(wanted.as_ref().unwrap_err().to_string(), e.to_string()),
            }
        }
    }
}
