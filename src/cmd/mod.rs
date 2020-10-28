use crate::frame::Frame;
use crate::db::Db;
use crate::connection::Connection;
use crate::shutdown::Shutdown;

#[derive(Debug)]
pub enum Command {
    Null
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        Ok(Command::Null)
    }

    pub(crate) async fn apply(&self, db: &Db, dst: &mut Connection, shutdown: &mut Shutdown) -> crate::Result<()> {
        Ok(())
    }
}