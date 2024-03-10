//! S3 Object operations impls

use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use axum::extract::multipart::Field;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use aws_sdk_s3::Client;

use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{field_parts, Error, FieldPart, PartSize};

/// A success result returned by `upload_object`.
pub enum UploadObjectOutput {
    Put(PutObjectOutput),
    Multipart(CompleteMultipartUploadOutput),
}

/// GetObjectOutput
///
/// This struct implements the `Stream` trait.
pub struct GetOutput {
    pub object: GetObjectOutput,
}

impl Stream for GetOutput {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.object.body)
            .poll_next(cx)
            .map_err(Error::ByteStreamError)
    }
}

impl GetOutput {
    /// Returns the object content-type, if the object content-type is no set
    /// it will return "application/octet-stream".
    pub fn content_type(&self) -> String {
        self.object
            .content_type
            .as_ref()
            .map(|typ| typ.to_owned())
            .unwrap_or_else(|| String::from("application/octet-stream"))
    }
}

impl From<GetObjectOutput> for GetOutput {
    fn from(object: GetObjectOutput) -> Self {
        Self { object }
    }
}

// ===== Multipart upload impls =====

/// Streams an object to S3 bucket.
#[derive(Debug)]
pub struct MultipartObject {
    /// The bucket the object is uploaded to.
    pub bucket: String,
    /// The upload id of the multipart upload.
    pub upload_id: String,
    /// The object key.
    pub key: String,
    /// The S3 client.
    pub client: Client,
    /// Size in which part in a multipart upload
    /// should uploaded in. The minimum part size is 5MB
    pub part_size: PartSize,
}

impl MultipartObject {
    /// Creates a new `MultipartObject`.
    ///
    /// # Errors
    ///
    /// This function will return if fails to initialize `CreateMultipartUploadOutput`
    pub async fn new(
        client: Client,
        bucket: String,
        key: String,
        content_type: String,
        part_size: PartSize,
    ) -> Result<Self, Error> {
        let res: CreateMultipartUploadOutput = client
            .create_multipart_upload()
            .content_type(content_type)
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(aws_sdk_s3::Error::from)?;

        let upload_id = res.upload_id().unwrap().to_owned();

        Ok(Self {
            bucket,
            upload_id,
            key,
            client,
            part_size,
        })
    }

    /// Uploads the object in parts
    pub async fn upload(&self, field: Field<'_>) -> Result<CompleteMultipartUploadOutput, Error> {
        self.start_upload_from(field_parts(field, self.part_size), None)
            .await
    }

    /// Uploads the object in parts starting with the given bytes
    pub async fn continue_upload(
        &self,
        field_parts: impl Stream<Item = FieldPart> + std::marker::Unpin + '_,
        bytes: Vec<u8>,
    ) -> Result<CompleteMultipartUploadOutput, Error> {
        self.start_upload_from(field_parts, Some(bytes)).await
    }

    /// Uploads the object in parts.
    #[inline]
    async fn start_upload_from(
        &self,
        mut field_parts: impl Stream<Item = FieldPart> + std::marker::Unpin + '_,
        content: Option<Vec<u8>>,
    ) -> Result<CompleteMultipartUploadOutput, Error> {
        let mut completed_parts = Vec::<CompletedPart>::new();
        let mut tasks = futures_util::stream::FuturesUnordered::new();

        // Upload the content already received.
        if let Some(content) = content {
            let parts = &mut content.chunks_exact(self.part_size.into()).enumerate();
            for (part_number, part) in parts.by_ref() {
                tasks.push(self.upload_part(part_number as i32 + 1, part.to_vec()));
            }
        }

        while let Some((part_number, result)) = field_parts.next().await {
            match result {
                Ok(part) => tasks.push(self.upload_part(part_number as i32 + 1, part)),
                Err(err) => return Err(Error::MultipartError(err.1)),
            }
        }

        while let Some(result) = tasks.next().await {
            let completed_part = result?;
            completed_parts.push(completed_part);
        }

        let output = self.complete_upload(completed_parts).await?;
        Ok(output)
    }

    /// Upload multipart chunk.
    pub async fn upload_part(
        &self,
        part_number: i32,
        part: Vec<u8>,
    ) -> Result<CompletedPart, Error> {
        let res = self
            .client
            .upload_part()
            .key(&self.key)
            .bucket(&self.bucket)
            .upload_id(&self.upload_id)
            .body(ByteStream::from(part))
            .part_number(part_number)
            .send()
            .await
            .map_err(aws_sdk_s3::Error::from)?;

        Ok(CompletedPart::builder()
            .part_number(part_number)
            .e_tag(res.e_tag().unwrap())
            .build())
    }

    /// Completes multipart upload.
    pub async fn complete_upload(
        &self,
        upload_parts: Vec<CompletedPart>,
    ) -> Result<CompleteMultipartUploadOutput, Error> {
        let completed_uploads: CompletedMultipartUpload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let res = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .multipart_upload(completed_uploads)
            .upload_id(&self.upload_id)
            .send()
            .await
            .map_err(aws_sdk_s3::Error::from)?;

        Ok(res)
    }

    /// Aborts multipart upload.
    pub async fn abort(&self) -> Result<AbortMultipartUploadOutput, Error> {
        let res = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(&self.upload_id)
            .send()
            .await
            .map_err(aws_sdk_s3::Error::from)?;

        Ok(res)
    }
}
