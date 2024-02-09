// Authors: Robert Lopez
// License: MIT (See `LICENSE.md`)
use std::fmt::{self, Debug};
use tokio::{sync::AcquireError, task::JoinError};

/// Error enum to wrap various errors that can occur inside the crate.
///
/// `Error::SdkError` is a formatted `"{:?}"` version of the `SdkError`
#[derive(Debug)]
pub enum Error {
    StdIo(std::io::Error),
    SdkError(String),
    Internal(String),
    JoinError(JoinError),
    AcquireError(AcquireError),
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
