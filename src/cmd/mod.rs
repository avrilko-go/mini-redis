mod get;

pub use get::Get;

mod unknown;

pub use unknown::Unknown;

use crate::frame::Frame;
use crate::db::Db;
use crate::connection::Connection;
use crate::shutdown::Shutdown;
use crate::parse::Parse;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?; // 这里一定传Frame::Array
        let command_name = parse.next_string()?.to_lowercase();// 转成小写
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };
        parse.finish()?; // 解析结束了
        Ok(command)
    }

    pub(crate) async fn apply(&self, db: &Db, dst: &mut Connection, shutdown: &mut Shutdown) -> crate::Result<()> {
        use Command::*;
        match self {
            Get(cmd) => cmd.apply(db, dst).await?,
            _ => {}
        }
        Ok(())
    }
}