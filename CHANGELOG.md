# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3](https://github.com/deepso7/minipaw/compare/v0.1.2...v0.1.3) - 2026-11-08

### Fixed

- address review feedback (round 2)
- address review feedback (round 1)
- *(sshd)* skip the cap's message and close once the client closed
- *(cli)* make the UI's session and chat input states explicit
- *(minipaw)* keep a pending handover across later attempts

## [0.1.2](https://github.com/deepso7/minipaw/compare/v0.1.1...v0.1.2) - 2026-10-31

### Added

- *(cli)* serve --send-peer-id tells the target who is connecting

### Other

- bump minip2p-rs to 0.11.1 and tokio to 1.54
- update Cargo.toml dependencies

## [0.1.1](https://github.com/deepso7/minipaw/compare/v0.1.0...v0.1.1) - 2026-10-21

### Added

- test hooks for the dead-link and admission timeouts

### Other

- bump minip2p-rs to 0.10.1
