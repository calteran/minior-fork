// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use std::fmt::{self, Debug};

/// Error enum to wrap various errors that can occur inside the crate.
#[derive(Debug, Clone)]
pub enum Error {
    StdIo(std::io::ErrorKind),
    SdkError(String),
    Internal(String),
    JoinError(String),
    AcquireError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Constructs a `Error::Internal` from `message`
    ///
    /// ---
    /// Example Usage
    /// ```
    ///
    /// let error: Error = Error::internal("Some internal error");
    /// ```
    pub fn internal(message: &str) -> Self {
        Self::Internal(message.to_string())
    }

    /// Constructs a `Error::SdkError` from `err`
    ///
    /// ---
    /// Example Usage
    /// ```
    ///
    /// let err: SdkError = ...;
    ///
    /// let error: Error = Error::sdk(err);
    /// ```
    pub fn sdk<E>(err: E) -> Self
    where
        E: Debug,
    {
        Self::SdkError(format!("{:?}", err))
    }
}
