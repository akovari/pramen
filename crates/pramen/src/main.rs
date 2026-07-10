//! The `pramen` CLI and daemon entry point.
//!
//! Skeleton: prints version information until the Phase 1 CLI tasks
//! (P1.15–P1.18) land. Argument parsing is deliberately dependency-free at
//! this stage.

use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None | Some("--version") | Some("-V") | Some("version") => {
            println!("pramen {}", pramen_core::VERSION);
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("pramen: unknown command `{other}`");
            eprintln!(
                "The CLI surface (validate, explain, run, ai evaluate) is not implemented yet."
            );
            ExitCode::FAILURE
        }
    }
}
