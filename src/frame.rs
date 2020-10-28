use bytes::{Bytes, Buf};
use std::io::Cursor;
use std::convert::TryInto;

#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl Frame {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'*' => { // 这是在redis中的意思是后面带一个数字，数字表示该条消息字段的总和
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'_' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' = peer_u8(src)? {
                    // Skip '-1\r\n'
                    skip(src, 4)
                } else {
                    let len = get_decimal(src)?.try_into()?;
                    skip(src, (len + 2))
                }
            }

            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into())
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            b'+' => { // 单行字符串
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            b':' => { // 整数
                let line = get_decimal(src)?;
                Ok(Frame::Integer(line))
            }
            b'-' => { // 错误信息
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            b'$' => {
                if b'-' == peer_u8(src)? { // 等于null的情况啊
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }
                    Ok(Frame::Null)
                } else {

                }
            }
            _ => unimplemented!()
        }
    }
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}

fn peer_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.bytes()[0])
}


// 从buff里面拿出第一个元素并且使原始数据截取掉拿出的部分
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;
    let line = get_line(src)?;
    atoi::<u64>(line).ok_or_else(|| {
        "protocol error; invalid frame format".into()
    })
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;
    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src[start..i]);
        }
    }
    Err(Error::Incomplete)
}