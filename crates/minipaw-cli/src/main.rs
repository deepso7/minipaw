//! minipaw: netcat between two machines over minip2p — QUIC, relayed
//! bootstrap, and hole-punched direct paths — with no accounts and no
//! control plane. Connection details travel out of band as a ticket.

mod args;
mod serve;
mod ssh;
mod ui;

use std::process::ExitCode;

use args::{Args, Command};
use ui::{Launch, term};

fn main() -> ExitCode {
    let argv: Vec<_> = std::env::args_os().collect();
    if let Some((tool, args)) = ssh::split_argv(&argv) {
        return ssh::run(tool, args);
    }
    // Usage errors exit 2, --help and --version 0.
    let args = Args::try_from_argv(argv).unwrap_or_else(|e| e.exit());
    match &args.command {
        Some(Command::Parse { ticket }) => {
            args::print_ticket(ticket);
            return ExitCode::SUCCESS;
        }
        // The ticket alone on stdout, so `$(minipaw ticket)` works.
        Some(Command::Ticket { identity, relay }) => {
            return match args::stable_ticket(identity.as_deref(), relay.as_deref()) {
                Ok((ticket, path, created)) => {
                    eprintln!("{}", args::identity_line(&path, created));
                    println!("{ticket}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("minipaw: {e}");
                    ExitCode::FAILURE
                }
            };
        }
        Some(Command::Serve {
            forward,
            identity,
            new,
            max_sessions,
            relay,
        }) => {
            ui::log::install(args.verbose, false);
            return serve::run(&serve::Options {
                forward,
                identity: identity.as_deref(),
                new: *new,
                max_sessions: *max_sessions,
                relay: relay.as_deref(),
            });
        }
        Some(Command::Ssh { args }) => return ssh::run(ssh::Tool::Ssh, args),
        Some(Command::Cp { args }) => return ssh::run(ssh::Tool::Cp, args),
        None => {}
    }
    term::install_panic_hook();
    ui::log::install(args.verbose, args.quiet);
    let mut config = match args::config(&args) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("minipaw: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(path) = &args.identity {
        match args::load_identity(Some(path), false) {
            // An existing identity is the usual case; only a new one is news.
            Ok((identity, path, created)) => {
                if created && !args.quiet {
                    eprintln!("{}", args::identity_line(&path, created));
                }
                config.identity = Some(identity);
            }
            Err(e) => {
                eprintln!("minipaw: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let mode = ui::choose_mode(args.quiet, args.plain);
    let result = ui::run(mode, Launch::new(args.ticket, config, args.verbose));
    // Errors print on a restored terminal.
    term::restore();
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(minipaw::Error::Stopped) => ExitCode::from(130),
        Err(e) => {
            match e {
                minipaw::Error::Input(e) => eprintln!("minipaw: reading stdin: {e}"),
                minipaw::Error::Output(e) => eprintln!("minipaw: writing stdout: {e}"),
                e => eprintln!("minipaw: {e}"),
            }
            ExitCode::FAILURE
        }
    }
}
