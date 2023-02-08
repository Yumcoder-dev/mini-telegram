//! Publish to a telegram channel example.
//!
//! A simple client that connects to a mini-telegram server, and
//! publishes a message on `foo` channel
//!
//! You can test this out by running:
//!
//!     cargo run --bin mini-telegram-server
//!
//! Then in another terminal run:
//!
//!     cargo run --example sub
//!
//! And then in another terminal run:
//!
//!     cargo run --example pub

#![warn(rust_2018_idioms)]

use mini_telegram::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Open a connection to the mini-telgram address.
    let mut client = client::connect("127.0.0.1:6379").await?;

    // publish message `bar` on channel foo
    client.publish("foo", "bar".into()).await?;

    Ok(())
}
