use std::str::Bytes;

use tokio::{fs, io::AsyncWriteExt};

use crate::storage::{Storage, Result, StorageError};

impl Storage {
    /// Put an object into storage
    pub async fn put_object(&self, bucket: &str, key:&str, data: Bytes) -> Result<()> {
        self.validate_bucket_name(bucket)?;
        self.validate_object_key(key)?;

        let bucket_path = self.get_bucket_path(bucket);

        if !bucket_path.exists() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        let object_path = self.get_object_path(bucket, key);

        if let Some(parent) = object_path.parent(){
            fs::create_dir_all(parent).await?;
        }

        //write file
        let mut file = fs::File::create(&object_path).await?;
        file.write_all(&data).await?;
        file.flush().await?;

        // Determine content type
        //todo
    }


    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(Bytes)> {
        self.validate_bucket_name(bucket)?;
        self.validate_object_key(key)?;

        let object_path = self.get_object_path(bucket, key);

        if !object_path.exists() {
            return Err(StorageError::ObjectNotFound(key.to_string()));
        }

       let data = fs::read(&object_path).await?;

       Ok((Bytes::from(data)))
    }
}
