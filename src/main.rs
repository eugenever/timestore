mod cli;
mod config;
mod log;
mod nats;
mod server;
mod storage;
mod utils;

use clap::{ Parser, Subcommand };
use cli::serve::command_serve;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Serve,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.cmd {
        Commands::Serve => {
            if let Err(err) = command_serve().await {
                eprintln!("Error 'serve' command: {}", err);
                exit(1);
            }
        }
    }
}
