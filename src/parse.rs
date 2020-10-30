use std::vec;
use crate::frame::Frame;
use std::fmt;
use std::fmt::Formatter;
use crate::cmd::Command;

#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>
}

#[derive(Debug)]
pub enum ParseError {
    EndOfStream,
    Other(crate::Error),
}


impl Parse {
    pub fn new(frame: Frame) -> Result<Self, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => {
                return Err(format!("protocol error; expected array, got {:?}", frame).into());
            }
        };
        Ok(Self {
            parts: array.into_iter() // 生成迭代器
        })
    }

    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => std::str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", frame).into()),
        }
    }

    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}


impl From<String> for ParseError {
    fn from(src: String) -> Self {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(e) => e.fmt(f)
        }
    }
}

impl std::error::Error for ParseError {}