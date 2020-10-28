use structopt::StructOpt;
use w::{DEFAULT_PORT, Result, server};
use tokio::net::{TcpListener};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    // 开启日志记录
    tracing_subscriber::fmt::try_init();
    let cli = Cli::from_args(); // 解析命令行参数
    let port = cli.port.as_deref().unwrap_or(DEFAULT_PORT);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    server::run(listener, signal::ctrl_c()).await
}

#[derive(Debug, StructOpt)]
#[structopt(name = "w-redis-server", version = env ! ("CARGO_PKG_VERSION"), author = env ! ("CARGO_PKG_AUTHORS"), about = "A Redis server")]
struct Cli {
    #[structopt(name = "port", long = "--port")]
    port: Option<String>
}
