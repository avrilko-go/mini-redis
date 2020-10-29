use crate::parse::Parse;
use crate::db::Db;
use crate::connection::Connection;
use crate::frame::Frame;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Get {
    key: String
}

impl Get {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string()
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    // 解析参数
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        let key = parse.next_string()?;

        Ok(Self {
            key
        })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.get(&self.key) {
            Frame::Bulk(value)
        } else {
            Frame::Null
        };

        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }
}