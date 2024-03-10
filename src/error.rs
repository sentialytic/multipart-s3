//! Multipart-s3 error.

use axum::extract::multipart::MultipartError;

/// Multipart-s3 error.
#[derive(Debug)]
pub enum Error {
    /// `multer::Error` error.
    MultipartError(MultipartError),
    /// `aws_sdk_s3::Error` error.
    S3Error(aws_sdk_s3::Error),
    /// `aws_sdk_s3::primitives::ByteStreamError` error.
    ByteStreamError(aws_sdk_s3::primitives::ByteStreamError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByteStreamError(err) => write!(f, "ByteStreamError: {err}"),
            Self::MultipartError(err) => write!(f, "MultipartError: {err}"),
            Self::S3Error(err) => write!(f, "S3Error: {err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<aws_sdk_s3::primitives::ByteStreamError> for Error {
    fn from(err: aws_sdk_s3::primitives::ByteStreamError) -> Self {
        Self::ByteStreamError(err)
    }
}

impl From<MultipartError> for Error {
    fn from(err: MultipartError) -> Self {
        Self::MultipartError(err)
    }
}

impl From<aws_sdk_s3::Error> for Error {
    fn from(err: aws_sdk_s3::Error) -> Self {
        Self::S3Error(err)
    }
}
