use std::{collections::HashMap, path::{Path, PathBuf}};
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::fs;
use serde::{Serialize, Deserialize};

use crate::db::{Database, DbError};


pub mod bucket;
pub mod object;



#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("Bucket not empty: {0}")]
    BucketNotEmpty(String),

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

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Invalid bucket name: {0}")]
    InvalidBucketName(String),

    #[error("Invalid object key: {0}")]
    InvalidObjectKey(String),
}


pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checksums {
    pub md5: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub object_count: usize,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub content_type: String,
    pub checksums: Checksums,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub custom_metadata: HashMap<String, String>,
}

#[derive(Clone)]
pub struct Storage {
    base_path: PathBuf,
    db: Database
}


impl Storage {

    pub async fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
         let metadata_path = base_path.join(".metadata");

         fs::create_dir_all(&base_path).await?;
         fs::create_dir_all(&metadata_path).await?;

        Ok(Self {
            base_path,
            metadata_path
        })
    }

    fn validate_bucket_name(&self, name: &str)-> Result<()> {
        if name.is_empty() || name.len() < 3 || name.len() > 30 {
            return Err(StorageError::InvalidBucketName("Bucket name must be between 3 and 30 characters".to_string()))
        }

        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return Err(StorageError::InvalidBucketName(
                "Bucket name can only contain alphanumeric characters, hyphens, and underscores".to_string()
            ));
        }

        Ok(())
    }


    fn get_bucket_path(&self, bucket: &str)-> PathBuf {
        self.base_path.join(bucket)
    }

    fn get_object_path(&self, bucket: &str, key: &str)-> PathBuf {
        self.base_path.join(bucket).join(key)
    }


    fn validate_object_key(&self, key: &str)-> Result<()> {
        if key.is_empty() || key.len() > 1024 {
            return Err(StorageError::InvalidObjectKey("Object key must be between 1 and 1024 characters".to_string()))
        }

        if key.starts_with('/') || key.ends_with('/') {
            return Err(StorageError::InvalidObjectKey(
                "Object key cannot start or end with /".to_string()
            ));
        }

        Ok(())
    }

}
