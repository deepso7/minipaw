//! `serve --ssh`: an SSH server inside minipaw, answering each session
//! itself instead of forwarding it to sshd. See docs/plans/builtin-ssh.md.

mod auth;
mod config;
mod process;
