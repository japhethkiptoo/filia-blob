use chrono::Utc;
use tokio::fs;

use crate::storage::{BucketInfo, StorageError};

use super:: {Storage, Result};

impl Storage {

    pub async fn create_bucket(&self, bucket_name:&str) -> Result<BucketInfo> {
        self.validate_bucket_name(bucket_name)?;

        let bucket_path = self.get_bucket_path(bucket_name);

        if bucket_path.exists() {
            return Err(StorageError::BucketAlreadyExists(bucket_name.to_string()));
        }

        fs::create_dir_all(&bucket_path).await?;

        Ok(BucketInfo {
            name: bucket_name.to_string(),
            created_at: Utc::now(),
            object_count: 0,
            total_size: 0
        })
    }


    /// delete a bucket - must be empty or force flag =true
    pub async fn delete_bucket(&self, bucket_name: &str, force: bool) -> Result<()> {
        let bucket_path = self.get_bucket_path(bucket_name);

        if !bucket_path.exists() {
            return Err(StorageError::BucketNotFound(bucket_name.to_string()))
        }

        if !force {
            let mut entries = fs::read_dir(&bucket_path).await?;

            if entries.next_entry().await?.is_some() {
                return Err(StorageError::BucketNotEmpty(bucket_name.to_string()))
            }
        }

        //delete all metadata files for the bucket
        //to-do
        //

        fs::remove_dir_all(&bucket_path).await?;


        Ok(())
    }


    /// list buckets
    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let mut buckets = Vec::new();
        let mut entries = fs::read_dir(&self.base_path).await?;

        while let Some(entry)= entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();

                // get created_at from metadata

                let (object_count, total_size) = self.get_bucket_stats(&name).await?;

                buckets.push(BucketInfo { name, created_at: Utc::now(), object_count, total_size })
            }
        }

        Ok(buckets)
    }


    async fn get_bucket_stats(&self, _bucket_name: &str) -> Result<(usize, u64)> {
        //todo calculate object count and total size
        Ok((0, 0))
    }
}
