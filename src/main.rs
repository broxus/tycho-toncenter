use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod cmd {
    pub mod run;
}

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(clippy::print_stderr)]
fn main() -> ExitCode {
    if std::env::var("RUST_BACKTRACE").is_err() {
        // Enable backtraces on panics by default.
        // SAFETY: Only a single thread is running yet.
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        // Disable backtraces in libraries by default
        // SAFETY: Only a single thread is running yet.
        unsafe { std::env::set_var("RUST_LIB_BACKTRACE", "0") };
    }

    match App::parse().run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("Error: {err:?}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Parser)]
#[clap(version = tycho_toncenter::version_string())]
#[clap(subcommand_required = true)]
pub struct App {
    #[clap(subcommand)]
    cmd: SubCmd,
}

impl App {
    pub fn run(self) -> Result<()> {
        match self.cmd {
            SubCmd::Run(cmd) => cmd.run(),
        }
    }
}

#[derive(Subcommand)]
enum SubCmd {
    Run(cmd::run::Cmd),
}
