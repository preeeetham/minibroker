//! A deliberately naive, *blocking*, one-client-at-a-time server.
//!
//! It exists only for comparison with the real Tokio server (see
//! DECISIONS.md, D20). It answers PING with PONG and logs everything else.
//!
//! Try it:
//!   cargo run --release --example blocking_server
//!   cargo run --release --example concurrent_ping -- 127.0.0.1:7879 50 1000
//! Clients are served strictly one after another: while the thread is
//! blocked in `read()` on client #1, nobody calls `accept()`, so the other
//! 49 sit in the kernel's accept backlog.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use bytes::BytesMut;
use minibroker::protocol::{Frame, FrameCodec};
use tokio_util::codec::{Decoder, Encoder};

fn serve(mut stream: TcpStream) -> std::io::Result<()> {
    // The codec is plain synchronous code, so the blocking server reuses it.
    let mut codec = FrameCodec::default();
    let mut rbuf = BytesMut::with_capacity(8 * 1024);
    let mut wbuf = BytesMut::new();
    let mut chunk = [0u8; 8 * 1024];
    loop {
        // Blocks this OS thread until the kernel has bytes for us.
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(()); // peer closed
        }
        rbuf.extend_from_slice(&chunk[..n]);
        loop {
            match codec.decode(&mut rbuf) {
                Ok(Some(Frame::Ping)) => codec
                    .encode(Frame::Pong, &mut wbuf)
                    .map_err(std::io::Error::other)?,
                Ok(Some(other)) => println!("frame: {other:?}"),
                Ok(None) => break,
                Err(e) => {
                    eprintln!("protocol error: {e}");
                    return Ok(());
                }
            }
        }
        if !wbuf.is_empty() {
            stream.write_all(&wbuf)?;
            wbuf.clear();
        }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7879")?;
    println!("blocking server on {}", listener.local_addr()?);
    for stream in listener.incoming() {
        let stream = stream?;
        let peer = stream.peer_addr()?;
        println!("client {peer} connected");
        // Serving inline: the accept loop is stuck here until this client
        // leaves. One slow client stalls every other client.
        if let Err(e) = serve(stream) {
            eprintln!("client {peer}: {e}");
        }
        println!("client {peer} disconnected");
    }
    Ok(())
}
