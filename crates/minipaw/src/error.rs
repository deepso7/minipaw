//! Why a session ended without completing.

use std::fmt;

/// Why [`Session::run`](crate::Session::run) failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// [`Handle::stop`](crate::Handle::stop) ended the session. The peer
    /// was told.
    Stopped,
    /// Reading the session's input failed. The peer was told.
    Input(std::io::Error),
    /// Writing the session's output failed. A closed reader is not a
    /// failure: the session goes on discarding output. The peer was told.
    Output(std::io::Error),
    /// The peer ended the session, refused it, or stopped.
    PeerEnded(String),
    /// The peer could not be reached, or went away and did not come back.
    Disconnected(String),
    /// The [`Config`](crate::Config) is invalid.
    Config(String),
    /// Anything else, such as a network or protocol error.
    Other(String),
}

impl Error {
    /// Converts the session code's internal errors, recognising the ones
    /// with a variant of their own.
    pub(crate) fn from_internal(error: Box<dyn std::error::Error>) -> Self {
        let error = match error.downcast::<Error>() {
            Ok(error) => return *error,
            Err(error) => error,
        };
        let error = match error.downcast::<crate::net::PeerEnded>() {
            Ok(ended) => return Error::PeerEnded(ended.0),
            Err(error) => error,
        };
        match error.downcast::<crate::net::Disconnected>() {
            Ok(gone) => Error::Disconnected(gone.0.to_owned()),
            Err(error) => Error::Other(error.to_string()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Stopped => f.write_str("stopped"),
            Error::Input(e) => write!(f, "reading input: {e}"),
            Error::Output(e) => write!(f, "writing output: {e}"),
            Error::PeerEnded(message)
            | Error::Disconnected(message)
            | Error::Config(message)
            | Error::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Input(e) | Error::Output(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_errors_keep_their_kind_and_message() {
        let ended: Box<dyn std::error::Error> =
            crate::net::PeerEnded("server refused: wrong token".into()).into();
        assert!(
            matches!(Error::from_internal(ended), Error::PeerEnded(m) if m == "server refused: wrong token")
        );
        let gone: Box<dyn std::error::Error> =
            crate::net::Disconnected("could not reach the server").into();
        assert!(matches!(Error::from_internal(gone), Error::Disconnected(_)));
        let other: Box<dyn std::error::Error> = "boom".into();
        assert_eq!(Error::from_internal(other).to_string(), "boom");
    }
}
