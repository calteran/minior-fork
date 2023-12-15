// Authors: Robert Lopez

use super::util::*;
use crate::error::Error;
use aws_sdk_s3::{presigning::PresignedRequest, Client};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct PresignedUploadManager {
    // TODO
}
