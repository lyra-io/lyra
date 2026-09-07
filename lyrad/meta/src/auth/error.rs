use crate::metadata::MetadataError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, AuthenticationError>;

#[derive(Debug, Error)]
pub enum AuthenticationError {
    #[error("invalid username or password")]
    InvalidCredentials,

    #[error(transparent)]
    Metadata(#[from] MetadataError),
}
