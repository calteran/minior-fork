// Authors: Robert Lopez

use std::fmt;

/// Error enum to wrap various errors that can occur inside the crate.
#[derive(Debug)]
pub enum Error {
    SdkError(String),
    Internal(String),
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
}
