# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/deepso7/minipaw/releases/tag/v0.1.0) - 2026-10-12

### Added

- *(web)* install script for the release binaries
- *(sshd)* the sftp subsystem
- *(cli)* minipaw serve --ssh
- *(cli)* the serve --ssh server: channels, admission and shutdown
- *(cli)* spawn, watch and tear down serve --ssh's programs
- *(cli)* authorized keys and russh settings for serve --ssh
- *(cli)* minipaw ssh and cp, the system ssh and scp through a ticket
- *(cli)* minipaw serve --forward, and -q for ProxyCommand
- *(sdk)* persistent identity for stable tickets
- *(cli)* tidier inline chat spacing
- *(cli)* simpler inline chat: a prompt and one status line
- *(cli)* inline chat box instead of taking over the screen

### Fixed

- say how to fix a missing authorized_keys, and why the host key changes
- cap SFTP writes at 256 KiB
- address review feedback (round 4)
- address review feedback (round 3)
- address review feedback (round 2)
- address review feedback (round 1)
- address review findings (round 3)
- address review findings (round 2)
- address review findings (round 1)
- address review feedback (round 3)
- address review feedback (round 2)
- address review feedback (round 1)
- address review findings (round 3)
- address review findings (round 2)
- address review findings (round 1)
- address review feedback (round 2)
- address review feedback (round 1)
- address review findings (round 3)
- address review findings (round 2)
- address review findings (round 1)
- address review findings (round 1)
- address review feedback (round 3)
- *(cli)* inline chat survives resizes; messages wrap natively
- *(cli)* -v with parse, remaining stdin length, linear input scroll
- address review feedback (round 2)
- address review feedback (round 1)
- address review findings (round 3)
- address review findings (round 2)
- address review findings (round 1)
- address review findings (round 1)

### Other

- release both crates and the CLI binaries with release-plz
- the host key warning comes from the server serve forwards to
- SFTP in the README and the plan
- docs, test, ci: document serve --ssh and check it end to end
- ssh lowercases the host key alias
- docs, test: minipaw ssh and cp in the README and an ssh_wrapper check
- end-to-end checks for serve and -q
- hook to shorten the server's resume timeout
- *(sdk)* multi-session server core behind listen
- *(cli)* remove the full-screen chat
- chat is an inline box now
- Drop dead-code allows left from before the panel and chat existed
- Show an inline status panel on stderr while stdout carries data
- Add the full-screen chat mode
- Build the CLI on clap with a mode-switching UI foundation
- Document the SDK and terminal UI, and check docs and the SDK in CI
- Move the session into the minipaw SDK behind a public API
- Report status as events and diagnostics through log
- Route session I/O, stop and progress through shared state
- Start the minipaw SDK crate with the pure modules
- Move into a Cargo workspace as minipaw-cli
- notice a silent link and resume on a fresh connection
- fix squatters timing, clarify resume skip, drop checkout credentials
- run the end-to-end checks against the hosted relay
- Add CI: fmt, clippy, unit tests and end-to-end checks on Linux and macOS
- Add README, a one-command check suite, and repeatable benchmarks
