// Authors: Robert Lopez

use std::fmt;

/// A simple error type to mimic `panic!` via the
/// `test_error!` macro when using `TestClient::run_test`.
///
/// ---
/// Example Usage:
/// ```
///
/// let test_client: TestClient = ...;
///
/// test_client.run_test(
///     |_| async {
///         let val_one: V = ...;
///         let val_two: V = ...;
///
///         if val_one != val_two {
///             // Ends test, by calling `Err(TestError(message))?`
///             test_error!("{:?} != {:?}", val_one, val_two);
///         }
///
///         Ok(())
///     }
///     None,
/// ).await.unwrap();
/// ```
#[derive(Debug)]
pub struct TestError(pub String);

impl TestError {
    pub fn from(message: &str) -> Self {
        Self(message.to_string())
    }
}

/// Macro to mimic `panic!` when using `TestClient::run_test`.
///
/// ---
/// Example Usage:
/// ```
///
/// let test_client: TestClient = ...;
///
/// test_client.run_test(
///     |_| async {
///         let val_one: V = ...;
///         let val_two: V = ...;
///
///         if val_one != val_two {
///             // Ends test, by calling `Err(TestError(message))?`
///             test_error!("{:?} != {:?}", val_one, val_two);
///         }
///
///         Ok(())
///     }
///     None,
/// ).await.unwrap();
/// ```
#[macro_export]
macro_rules! test_error {
    ($fmt:expr $(, $arg:expr)*) => {
        Err(TestError(format!($fmt $(, $arg)*)))?
    };
}

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::error::Error for TestError {}
