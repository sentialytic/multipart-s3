//! Field stream impls

use axum::extract::multipart::{Field, MultipartError};
use futures_util::{stream::TryChunksError, Stream, StreamExt, TryStreamExt};

use super::MIN_5MB;

// A type returned by `Field` `Stream`
pub(crate) type FieldPart = (usize, Result<Vec<u8>, TryChunksError<u8, MultipartError>>);

/// Return a `Stream` of the `Field` in parts of size `part_size`
#[inline]
pub(crate) fn field_parts(
    field: Field<'_>,
    part_size: PartSize,
) -> impl Stream<Item = FieldPart> + std::marker::Unpin + '_ {
    field
        .map_ok(|chunk| futures_util::stream::iter(chunk).map(Ok::<_, MultipartError>))
        .try_flatten()
        .try_chunks(part_size.into())
        .enumerate()
}

/// The size of a single part in a multipart upload.
///
/// The minimum part size is 5MB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartSize(usize);

impl From<PartSize> for usize {
    fn from(part_size: PartSize) -> Self {
        part_size.0
    }
}

impl From<usize> for PartSize {
    fn from(part_size: usize) -> Self {
        let part_size = if part_size < MIN_5MB {
            MIN_5MB
        } else {
            part_size
        };
        PartSize(part_size)
    }
}
