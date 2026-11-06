use clap::Parser;
use pg_osc::ctx::{Flags, Terminated};
use pg_osc::options::{Cli, Command, Options};
use pg_osc::{db, log, orchestrate};
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM, SIGUSR1};
use std::process::ExitCode;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Command::Swap(args) => {
            match orchestrate::request_swap(&args.conn.conn_opts(), &args.conn.schema, &args.table)
            {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    pg_osc::error!(format!("{e:#}"));
                    ExitCode::FAILURE
                }
            }
        }
        Command::Perform(args) => {
            log::set_verbose(args.verbose);
            let opts = match Options::from_args(&args) {
                Ok(o) => o,
                Err(e) => {
                    pg_osc::fatal!(format!("{e:#}"));
                    return ExitCode::FAILURE;
                }
            };
            let flags = Arc::new(Flags::default());
            if let Err(e) = spawn_signal_handler(flags.clone()) {
                pg_osc::fatal!(format!("Can't install signal handlers: {e:#}"));
                return ExitCode::FAILURE;
            }
            match orchestrate::run(opts, flags) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => match e.downcast_ref::<Terminated>() {
                    Some(t) => ExitCode::from((128 + t.0) as u8),
                    None => ExitCode::FAILURE,
                },
            }
        }
    }
}

/// USR1 requests the swap. TERM/INT/QUIT set the terminate flag and cancel
/// the running query on the main connection; the main thread then cleans up
/// (trigger, audit and shadow tables) and exits with 128+signal.
fn spawn_signal_handler(flags: Arc<Flags>) -> anyhow::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([SIGTERM, SIGINT, SIGQUIT, SIGUSR1])?;
    std::thread::spawn(move || {
        for sig in signals.forever() {
            if sig == SIGUSR1 {
                pg_osc::info!("Signal USR1 received, swap requested");
                flags.swap_requested.store(true, Ordering::SeqCst);
                continue;
            }
            if flags.terminate.swap(sig, Ordering::SeqCst) != 0 {
                pg_osc::warn!("Second signal received, exiting without cleanup");
                std::process::exit(128 + sig);
            }
            if let Some(token) = db::CANCEL.get() {
                if let Ok(tls) = db::tls() {
                    let _ = token.cancel_query(tls);
                }
            }
        }
    });
    Ok(())
}
