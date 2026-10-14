//! AES-CTR over a byte stream, as HDFS's `CryptoInputStream` /
//! `CryptoOutputStream` with `AES/CTR/NoPadding`: once a data transfer
//! connection has negotiated keys, every byte each way is XORed with the
//! AES keystream (a 128-bit big-endian counter starting at the IV). There
//! is no framing and no per-message MAC: blocks carry their own checksums.
//!
//! This is the fast path for encrypted data transfer: reads decrypt in
//! place; writes encrypt into one buffer that is kept across partial
//! writes, so each byte is encrypted exactly once. The `aes` crate uses
//! AES-NI / ARMv8 AES instructions when the CPU has them.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use aes::cipher::{KeyIvInit, StreamCipher};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Largest chunk of a write encrypted at once.
const WRITE_CHUNK: usize = 64 * 1024;

/// AES-CTR keystream with a 128- or 256-bit key.
pub enum AesCtr {
    Aes128(ctr::Ctr128BE<aes::Aes128>),
    Aes256(ctr::Ctr128BE<aes::Aes256>),
}

impl AesCtr {
    /// `key` is 16 or 32 bytes, `iv` 16.
    pub fn new(key: &[u8], iv: &[u8]) -> io::Result<Self> {
        let bad = || io::Error::new(io::ErrorKind::InvalidInput, format!("AES key of {} bytes / IV of {} bytes", key.len(), iv.len()));
        if iv.len() != 16 {
            return Err(bad());
        }
        Ok(match key.len() {
            16 => AesCtr::Aes128(ctr::Ctr128BE::new_from_slices(key, iv).map_err(|_| bad())?),
            32 => AesCtr::Aes256(ctr::Ctr128BE::new_from_slices(key, iv).map_err(|_| bad())?),
            _ => return Err(bad()),
        })
    }

    pub fn apply(&mut self, data: &mut [u8]) {
        match self {
            AesCtr::Aes128(c) => c.apply_keystream(data),
            AesCtr::Aes256(c) => c.apply_keystream(data),
        }
    }
}

/// A stream whose bytes are AES-CTR encrypted each way.
pub struct AesCtrStream<S> {
    inner: S,
    decrypt: AesCtr,
    encrypt: AesCtr,
    /// Encrypted bytes accepted from the caller, not yet written.
    pending: Vec<u8>,
    sent: usize,
}

impl<S> AesCtrStream<S> {
    pub fn new(inner: S, decrypt: AesCtr, encrypt: AesCtr) -> Self {
        Self { inner, decrypt, encrypt, pending: Vec::with_capacity(WRITE_CHUNK), sent: 0 }
    }
}

impl<S: AsyncWrite + Unpin> AesCtrStream<S> {
    /// Write out what is pending; Ready once it is all written.
    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.sent < self.pending.len() {
            let n = std::task::ready!(Pin::new(&mut self.inner).poll_write(cx, &self.pending[self.sent..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.sent += n;
        }
        self.pending.clear();
        self.sent = 0;
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for AesCtrStream<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        let before = buf.filled().len();
        std::task::ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        this.decrypt.apply(&mut buf.filled_mut()[before..]);
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for AesCtrStream<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        std::task::ready!(this.poll_pending(cx))?;
        let n = data.len().min(WRITE_CHUNK);
        this.pending.extend_from_slice(&data[..n]);
        this.encrypt.apply(&mut this.pending);
        // Start writing now; what the socket does not take stays pending.
        if let Poll::Ready(Err(e)) = this.poll_pending(cx) {
            return Poll::Ready(Err(e));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        std::task::ready!(this.poll_pending(cx))?;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        std::task::ready!(this.poll_pending(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    /// NIST SP 800-38A F.5.1, CTR-AES128.Encrypt (first two blocks).
    #[test]
    fn matches_the_nist_ctr_vector() {
        let mut c = AesCtr::new(&hex("2b7e151628aed2a6abf7158809cf4f3c"), &hex("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff")).unwrap();
        let mut data = hex("6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e51");
        c.apply(&mut data[..5]);
        c.apply(&mut data[5..]);
        assert_eq!(data, hex("874d6191b620e3261bef6864990db6ce9806f66b7970fdff8617187bb9fffdff"));
        assert!(AesCtr::new(&[0; 24], &[0; 16]).is_err());
    }

    #[tokio::test]
    async fn both_directions_round_trip_through_small_and_large_writes() {
        let (a, b) = tokio::io::duplex(4096);
        let (k1, iv1, k2, iv2) = ([1u8; 16], [2u8; 16], [3u8; 32], [4u8; 16]);
        // a writes with (k1, iv1) and reads with (k2, iv2); b the reverse.
        let mut a = AesCtrStream::new(a, AesCtr::new(&k2, &iv2).unwrap(), AesCtr::new(&k1, &iv1).unwrap());
        let mut b = AesCtrStream::new(b, AesCtr::new(&k1, &iv1).unwrap(), AesCtr::new(&k2, &iv2).unwrap());
        let big: Vec<u8> = (0..300_000u32).map(|i| (i * 7) as u8).collect();
        let sent = big.clone();
        let writer = tokio::spawn(async move {
            a.write_all(b"hello").await.unwrap();
            a.write_all(&sent).await.unwrap();
            a.flush().await.unwrap();
            let mut reply = [0u8; 2];
            a.read_exact(&mut reply).await.unwrap();
            reply
        });
        let mut got = vec![0u8; 5 + big.len()];
        b.read_exact(&mut got).await.unwrap();
        assert_eq!(&got[..5], b"hello");
        assert_eq!(&got[5..], &big[..]);
        b.write_all(b"ok").await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(&writer.await.unwrap(), b"ok");
    }

    #[tokio::test]
    async fn the_wire_carries_ciphertext() {
        let (a, mut raw) = tokio::io::duplex(1024);
        let mut a = AesCtrStream::new(a, AesCtr::new(&[9; 16], &[0; 16]).unwrap(), AesCtr::new(&[8; 16], &[0; 16]).unwrap());
        a.write_all(b"secret block data").await.unwrap();
        a.flush().await.unwrap();
        let mut seen = [0u8; 17];
        raw.read_exact(&mut seen).await.unwrap();
        assert_ne!(&seen, b"secret block data");
    }
}
