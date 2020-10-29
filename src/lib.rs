pub mod server;
pub mod db;
pub mod shutdown;
pub mod connection;
pub mod frame;
pub mod cmd;
pub mod parse;


// redis-server 默认监听端口
pub const DEFAULT_PORT: &str = "6378";

// 自定义redis的Error(使用鸭子类型，只要实现了线程安全的error都可以)
pub type Error = Box<dyn Send + Sync + std::error::Error>;

// 自定义redis的Result
pub type Result<T> = std::result::Result<T, Error>;