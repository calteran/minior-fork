// Authors: Robert Lopez

use std::fmt::{self, Debug};
use tokio::task::JoinError;

/// Error enum to wrap various errors that can occur inside the crate.
#[derive(Debug)]
pub enum Error {
    StdIo(std::io::Error),
    SdkError(String),
    Internal(String),
    JoinError(JoinError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn internal(message: &str) -> Self {
        Self::Internal(message.to_string())
    }

    pub fn sdk<E>(err: E) -> Self
    where
        E: Debug,
    {
        Self::SdkError(format!("{:?}", err))
    }
}
