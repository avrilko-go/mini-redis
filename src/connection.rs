use tokio::net::TcpStream;
use tokio::io::{BufWriter, AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, Buf};
use crate::frame::Frame;
use std::io::Cursor;


#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(1024 * 4),
        }
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }


        Ok(Some(Frame::Null))
    }

    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use crate::frame::Error::Incomplete;
        // 新建游标
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                // 开始解析了
                let frame = Frame::parse(&mut buf)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into())
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;
                for entry in &**val {
                    self.write_value(entry);
                }
            }
            _ => self.write_value(frame).await?
        }

        Ok(())
    }

    async fn write_value(&mut self, frame: &Frame) -> std::io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unreachable!()
        }
        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> std::io::Result<()> {
        use std::io::Write;

        let mut buff = [0u8; 12];
        let mut buff = Cursor::new(&mut buff[..]);
        write!(buff, "{}", val)?;
        let position = buff.position() as usize;
        self.stream.write_all(&buff.get_ref()[..position]).await?;
        self.stream.write_all(b"\r\n").await?;
        Ok(())
    }
}