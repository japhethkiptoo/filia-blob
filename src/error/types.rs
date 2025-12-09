use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object already exists: {0}")]
    ObjectAlreadyExists(String),

    #[error("Duplicate content detected: {0}")]
    DuplicateContent(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),

    #[error("Invalid bucket name: {0}")]
    InvalidBucketName(String),

    #[error("Invalid object key: {0}")]
    InvalidObjectKey(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}


#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {0}")]
    SqlxError(#[from] sqlx::Error),

    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
pub type DbResult<T> = std::result::Result<T, DbError>;
