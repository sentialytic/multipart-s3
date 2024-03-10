//! # Multipart S3
//!
//! The multipart-s3 is an axum crate that provides a simple interface to upload, get and delete objects from S3.
//!
//! ## Features
//!
//! - Upload objects to S3 using multipart upload for large files or put object for smaller files.
//! - Retrieve objects from S3 with support for streaming the object content.
//! - Delete single or multiple objects from S3.
//! - Seamless integration with the `axum` web framework.
//! - Customizable part size for multipart uploads.
//! - Automatic handling of multipart upload completion or abortion in case of errors.
//!
//! ## Getting Started
//!
//! To use the `multipart-s3` crate in your Rust project, add the following to your `Cargo.toml` file:
//!
//! ```toml
//! [dependencies]
//! multipart-s3 = "0.1.0"
//! ```
//!
//! Then, you can use the crate in your Rust code:
//!
//! ```rust
//! use multipart_s3::{Client, upload_object, get_object, remove_object, delete_objects};
//! use axum::extract::multipart::Field;
//!
//! // Create an S3 client
//!#[tokio::main]
//! async fn main() {
//!     let config = aws_config::load_from_env().await;
//!     let client = Client::new(&config);
//!
//!     // Upload an object to S3
//!     let field: Field = todo!();// Obtain the field from axum multipart
//!     let bucket = "my-bucket".to_string();
//!     let key = "my-object-key".to_string();
//!     let content_type = "application/octet-stream".to_string();
//!     let part_size = 5 * 1024 * 1024; // 5MB
//!     let stream_from = 10 * 1024 * 1024; // 10MB
//!     let result = upload_object(&client, field, bucket, key, content_type, part_size, stream_from).await;
//!
//!     // Retrieve an object from S3
//!     let bucket = "my-bucket";
//!     let key = "my-object-key";
//!     let result = get_object(&client, bucket, key).await;
//!
//!     // Delete an object from S3
//!     let bucket = "my-bucket";
//!     let key = "my-object-key";
//!     let result = remove_object(&client, bucket, key).await;
//!
//!     // Delete multiple objects from S3
//!     let bucket = "my-bucket";
//!     let object_keys = vec!["key1".to_string(), "key2".to_string()];
//!     let result = delete_objects(&client, bucket, object_keys).await;
//! }
//! ```
//!
//! For more detailed usage and examples, please refer to the documentation of individual functions and types.
//!
//! ## Error Handling
//!
//! The `multipart-s3` crate defines a custom `Error` enum that encompasses various error types that can occur during S3 operations. These include:
//!
//! - `MultipartError`: Errors related to multipart form data handling.
//! - `S3Error`: Errors specific to the S3 API, such as invalid requests or server errors.
//! - `ByteStreamError`: Errors related to streaming object content.
//!
//! When using the functions provided by this crate, you can handle errors by matching on the `Error` enum and taking appropriate actions based on the error variant.
//!
//! ## Streaming Support
//!
//! The `get_object` function returns a `GetObject` struct that implements the `Stream` trait from the `futures` crate. This allows you to stream the object content efficiently without loading the entire object into memory at once.

#![forbid(unsafe_code)]

use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;

use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::operation::get_object::GetObjectError;

use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use axum::extract::multipart::Field;

use futures_util::StreamExt;

pub use aws_config;
pub use aws_sdk_s3::Client;

mod error;
mod field;
pub mod objects;

pub use error::Error;
pub use field::PartSize;
use field::{field_parts, FieldPart};
use objects::{GetOutput, MultipartObject, UploadObjectOutput};

/// The size of 5MB.
const MIN_5MB: usize = 1024 * 1024 * 5;

/// Uploads an object to S3.
///
/// This function provides a convenient way to upload an object to S3 using either a single-part upload or a multipart upload,
/// depending on the size of the object. If the object size is greater than or equal to `stream_from`, it will be uploaded
/// using a multipart upload; otherwise, it will be uploaded in a single request using `put_object`.
///
/// # Arguments
///
/// * `client` - The S3 client used to perform the upload.
/// * `field` - The multipart field containing the object data.
/// * `bucket` - The name of the S3 bucket to upload the object to.
/// * `key` - The key (path) of the object in the S3 bucket.
/// * `content_type` - The content type of the object being uploaded.
/// * `part_size` - The size of each part in bytes for a multipart upload.
/// * `stream_from` - The minimum object size in bytes to trigger a multipart upload.
///
/// # Errors
///
/// This function will return an error if it fails to upload the object. Possible errors include:
/// - `Error::MultipartError` if there is an error processing the multipart field.
/// - `Error::S3Error` if there is an error communicating with the S3 service.
///
/// # Panics
///
/// This function will panic if `stream_from` is not a multiple of `part_size`.
pub async fn upload_object(
    client: &Client,
    field: Field<'_>,
    bucket: String,
    key: String,
    content_type: String,
    part_size: usize,
    stream_from: usize,
) -> Result<UploadObjectOutput, Error> {
    assert!(
        stream_from % part_size == 0,
        "stream_from must be a multiple of part_size"
    );

    let part_size = PartSize::from(part_size);

    let mut content = Vec::with_capacity(stream_from);
    let mut content_size = 0;
    let mut stream_object = false;
    let mut field_iter = field_parts(field, part_size);

    while let Some((_part_number, result)) = field_iter.next().await {
        match result {
            Ok(part) => {
                content_size += part.len();
                content.extend(part);
                if content_size == stream_from {
                    stream_object = true;
                    break;
                }
            }
            Err(err) => return Err(Error::MultipartError(err.1)),
        }
    }

    if stream_object {
        // Upload an object in parts (multipart upload)
        let obj =
            MultipartObject::new(client.clone(), bucket, key, content_type, part_size).await?;
        match obj.continue_upload(field_iter, content).await {
            Ok(output) => Ok(UploadObjectOutput::Multipart(output)),
            Err(err) => {
                obj.abort().await?;
                Err(err)
            }
        }
    } else {
        // Upload an object in one shot (put object)
        let output = put_object(client, content, &bucket, &key, &content_type).await?;
        Ok(UploadObjectOutput::Put(output))
    }
}

/// Uploads an object to S3 using a single-part upload.
///
/// This function uploads an object to S3 using the `PutObject` operation, which is suitable for smaller objects
/// that can be uploaded in a single request.
///
/// # Arguments
///
/// * `client` - The S3 client used to perform the upload.
/// * `bytes` - The bytes of the object to upload.
/// * `bucket` - The name of the S3 bucket to upload the object to.
/// * `key` - The key (path) of the object in the S3 bucket.
/// * `content_type` - The content type of the object being uploaded.
pub async fn put_object(
    client: &Client,
    bytes: Vec<u8>,
    bucket: &str,
    key: &str,
    content_type: &str,
) -> Result<PutObjectOutput, Error> {
    let res = client
        .put_object()
        .content_type(content_type)
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .map_err(aws_sdk_s3::Error::from)?;

    Ok(res)
}

/// Uploads an object to S3 using a multipart upload.
///
/// This function uploads an object to S3 using the multipart upload process, which is suitable for larger objects
/// that need to be uploaded in multiple parts. It streams the object data from the provided `Field` and uploads
/// each part concurrently.
///
/// # Arguments
///
/// * `client` - The S3 client used to perform the upload.
/// * `field` - The multipart field containing the object data.
/// * `bucket` - The name of the S3 bucket to upload the object to.
/// * `key` - The key (path) of the object in the S3 bucket.
/// * `content_type` - The content type of the object being uploaded.
/// * `part_size` - The size of each part in bytes for the multipart upload.
pub async fn multipart_object(
    client: Client,
    field: Field<'_>,
    bucket: String,
    key: String,
    content_type: String,
    part_size: usize,
) -> Result<CompleteMultipartUploadOutput, Error> {
    let obj = MultipartObject::new(client, bucket, key, content_type, part_size.into()).await?;
    match obj.upload(field).await {
        Ok(output) => Ok(output),
        Err(err) => {
            obj.abort().await?;
            Err(err)
        }
    }
}

/// Get an object from an S3 bucket.
///
/// This function retrieves an object from the specified S3 bucket using the provided object key.
/// If the object is found, it returns a `GetObject` struct wrapped in an `Option`. The `GetObject`
/// struct contains the `GetObjectOutput` from the S3 client, which includes the object's data and
/// metadata.
///
/// If the specified object key does not exist in the bucket, this function returns `None`.
///
/// # Arguments
///
/// * `client` - A reference to the S3 client.
/// * `bucket` - The name of the S3 bucket.
/// * `key` - The key of the object to retrieve.
///
/// # Returns
///
/// Returns `Ok(Some(GetObject))` if the object is found, where `GetObject` contains the retrieved
/// object's data and metadata. Returns `Ok(None)` if the object key does not exist in the bucket.
/// Returns an `Err` if an error occurs during the operation.
pub async fn get_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Option<GetOutput>, Error> {
    let response = client.get_object().bucket(bucket).key(key).send().await;

    let object = match response {
        Ok(obj) => obj,
        Err(err) => match err.into_service_error() {
            GetObjectError::NoSuchKey(_) => {
                return Ok(None);
            }
            err => return Err(Error::S3Error(aws_sdk_s3::Error::from(err))),
        },
    };

    Ok(Some(GetOutput { object }))
}

/// Deletes an object from an S3 bucket.
///
/// This function deletes a single object from the specified S3 bucket using the provided object key.
///
/// # Arguments
///
/// * `client` - A reference to the S3 client.
/// * `bucket` - The name of the S3 bucket.
/// * `key` - The key of the object to delete.
pub async fn remove_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<DeleteObjectOutput, Error> {
    let res = client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(aws_sdk_s3::Error::from)?;

    Ok(res)
}

/// Deletes multiple objects from an S3 bucket.
///
/// This function deletes a list of objects from the specified S3 bucket using their object keys.
///
/// If the `object_keys` vector is empty, this function returns `Ok(None)` without making any
/// deletions.
///
/// # Arguments
///
/// * `client` - A reference to the S3 client.
/// * `bucket` - The name of the S3 bucket.
/// * `object_keys` - A vector of object keys to delete.
///
/// # Returns
///
/// Returns `Ok(Some(DeleteObjectsOutput))` if the objects are successfully deleted. The
/// `DeleteObjectsOutput` contains information about the deleted objects. Returns `Ok(None)` if
/// the `object_keys` vector is empty.
pub async fn delete_objects(
    client: &Client,
    bucket: &str,
    object_keys: Vec<String>,
) -> Result<Option<DeleteObjectsOutput>, Error> {
    if object_keys.is_empty() {
        return Ok(None);
    }

    // construct object identifiers
    let object_identifiers = object_keys
        .into_iter()
        .map(|key| {
            ObjectIdentifier::builder()
                .set_key(Some(key))
                .build()
                .map_err(aws_sdk_s3::Error::from)
        })
        .collect::<Result<Vec<ObjectIdentifier>, aws_sdk_s3::Error>>()?;

    let res = client
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .set_objects(Some(object_identifiers))
                .build()
                .map_err(aws_sdk_s3::Error::from)?,
        )
        .send()
        .await
        .map_err(aws_sdk_s3::Error::from)?;

    Ok(Some(res))
}

// // ===== Multipart impls =====

// /// Extract a `multer::Multipart` from the request.
// ///
// /// # Returns
// ///
// /// `None` if the request is not a `multipart/form-data` request.
// pub async fn multipart_from_request(
//     req: axum::extract::Request,
// ) -> Option<multer::Multipart<'static>> {
//     let boundary = req
//         .headers()
//         .get(axum::http::header::CONTENT_TYPE)
//         .and_then(|ct| ct.to_str().ok())
//         .and_then(|ct| multer::parse_boundary(ct).ok())?;
//     let stream = req.with_limited_body().into_body();
//     let multipart = multer::Multipart::new(stream.into_data_stream(), boundary);
//     Some(multipart)
// }

// use futures_util::StreamExt;
// use http_body_util::{BodyStream};
// use hyper::{body::Incoming, header::CONTENT_TYPE, Request};
//
// pub async fn multipart_from_hyper(req: Request<Incoming>) -> Option<multer::Multipart<'static>> {
//     let boundary = req
//         .headers()
//         .get(CONTENT_TYPE)
//         .and_then(|ct| ct.to_str().ok())
//         .and_then(|ct| multer::parse_boundary(ct).ok())?;

//     let body_stream = BodyStream::new(req.into_body())
//         .filter_map(|result| async move { result.map(|frame| frame.into_data().ok()).transpose() });

//     Some(multer::Multipart::new(body_stream, boundary))
// }

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{FromRequest, Multipart, Request};
    use axum::http::header::CONTENT_TYPE;
    use dotenvy::dotenv;

    // Helper function to create a test S3 client
    async fn create_test_client() -> Client {
        let _ = dotenv();

        let config = aws_config::load_from_env().await;
        let config = config
            .to_builder()
            .endpoint_url(std::env::var("S3_ENDPOINT").unwrap())
            .build();

        Client::new(&config)
    }

    // Helper function to create a test multipart field
    async fn create_test_field() -> Multipart {
        let content_type = "multipart/form-data; boundary=X-BOUNDARY";

        let data = "--X-BOUNDARY\r\nContent-Disposition: form-data; \
        name=\"test-key\"\r\n\r\nabcd\r\n--X-BOUNDARY--\r\n";

        let body = axum::body::Body::from(data);

        let req = Request::builder()
            .header(CONTENT_TYPE, content_type)
            .body(body)
            .unwrap();

        Multipart::from_request(req, &()).await.unwrap()
    }

    #[tokio::test]
    async fn test_upload_object() {
        let client = create_test_client().await;

        let bucket = "test-bucket".to_string();
        let key = "test_upload_object-key".to_string();
        let content_type = "text/plain".to_string();
        let part_size = 5 * 1024 * 1024; // 5MB
        let stream_from = 10 * 1024 * 1024; // 10MB

        // Create a test multipart field
        let mut field = create_test_field().await;
        let field = field.next_field().await.unwrap().unwrap();

        // Upload the object
        let result = upload_object(
            &client,
            field,
            bucket,
            key,
            content_type,
            part_size,
            stream_from,
        )
        .await;

        assert!(result.is_ok());

        // Cleanup: Delete the uploaded object
        let _ = remove_object(&client, "test-bucket", "test-key").await;
    }

    #[tokio::test]
    async fn test_put_object() {
        let client = create_test_client().await;

        let bucket = "test-bucket";
        let key = "test_put_object-key";
        let content_type = "text/plain";
        let bytes = "Hello, World!".as_bytes().to_vec();

        // Put the object
        let result = put_object(&client, bytes, bucket, key, content_type).await;
        assert!(result.is_ok());

        // Cleanup: Delete the uploaded object
        let _ = remove_object(&client, bucket, key).await;
    }

    #[tokio::test]
    async fn test_multipart_object() {
        let client = create_test_client().await;

        let bucket = "test-bucket".to_string();
        let key = "test_multipart_object-key".to_string();
        let content_type = "text/plain".to_string();
        let part_size = 5 * 1024 * 1024; // 5MB

        // Create a test multipart field
        let mut field = create_test_field().await;

        // Upload the object using multipart upload
        let result = multipart_object(
            client.clone(),
            field.next_field().await.unwrap().unwrap(),
            bucket.to_owned(),
            key.to_owned(),
            content_type,
            part_size,
        )
        .await;
        assert!(result.is_ok());

        // Cleanup: Delete the uploaded object
        let _ = remove_object(&client, &bucket, &key).await;
    }

    #[tokio::test]
    async fn test_get_object() {
        let client = create_test_client().await;
        let bucket = "test-bucket";
        let key = "test_get_object-key";
        let content_type = "text/plain";
        let content = "Hello, world!".as_bytes().to_vec();

        // Upload the test object
        let _ = put_object(&client, content.clone(), bucket, key, content_type).await.unwrap();

        // Get the object
        let result = get_object(&client, bucket, key).await.unwrap();
        assert!(result.is_some());

        let mut object = result.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = object.next().await {
            data.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(data, content);

        // Clean up the test object
        let _ = remove_object(&client, bucket, key).await.unwrap();
    }

    #[tokio::test]
    async fn test_remove_object() {
        let client = create_test_client().await;
        let bucket = "test-bucket";
        let key = "test_remove_object-key";
        let content_type = "text/plain";
        let content = "Hello, world!".as_bytes().to_vec();

        // Upload the test object
        let _ = put_object(&client, content, bucket, key, content_type).await.unwrap();

        // Remove the object
        let result = remove_object(&client, bucket, key).await.unwrap();
        assert_eq!(result.delete_marker(), None);

        // Verify the object is removed
        let result = get_object(&client, bucket, key).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete_objects() {
        let client = create_test_client().await;
        let bucket = "test-bucket";
        let keys = vec!["test_delete_objects-key1".to_string(), "test_delete_objects-key2".to_string()];
        let content_type = "text/plain";
        let content = "Hello, world!".as_bytes().to_vec();

        // Upload the test objects
        for key in &keys {
            let _ = put_object(&client, content.clone(), bucket, key, content_type).await.unwrap();
        }

        // Delete the objects
        let result = delete_objects(&client, &bucket, keys.clone())
            .await
            .unwrap();
        assert!(result.is_some());

        // Verify the objects are deleted
        for key in &keys {
            let result = get_object(&client, &bucket, key).await.unwrap();
            assert!(result.is_none());
        }
    }
}
