use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::{SqlitePool, sqlite::{SqlitePoolOptions, SqliteRow}, Row,migrate};
use thiserror::Error;


#[derive(Error, Debug)]
pub enum DbError {
    #[error("Database error: {0}")]
    SqlxError(#[from] sqlx::Error),

    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),
}


pub type Result<T> = std::result::Result<T, DbError>;

#[derive(Debug, Clone)]
pub struct BucketRecord {
    pub id: i64,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}



#[derive(Debug, Clone)]
pub struct ObjectRecord {
    pub id: i64,
    pub bucket_id: i64,
    pub key: String,
    pub size: i64,
    pub content_type: String,
    pub md5_checksum: String,
    pub sha256_checksum: String,
    pub storage_path: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool
}



impl Database {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        // Run migrations
        migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }



    pub async fn create_bucket(&self, name: &str) -> Result<BucketRecord> {
        let now = Utc::now();

        let result = sqlx::query(
            "INSERT INTO buckets (name, created_at, updated_at) VALUES (?, ?, ?)"
        )
        .bind(name)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(BucketRecord {
            id: result.last_insert_rowid(),
            name: name.to_string(),
            created_at: now,
            updated_at: now,
        })
    }


    pub async fn get_bucket(&self, name: &str) -> Result<BucketRecord> {
           let row = sqlx::query(
               "SELECT id, name, created_at, updated_at FROM buckets WHERE name = ?"
           )
           .bind(name)
           .fetch_optional(&self.pool)
           .await?
           .ok_or_else(|| DbError::BucketNotFound(name.to_string()))?;

           Ok(BucketRecord {
               id: row.get("id"),
               name: row.get("name"),
               created_at: row.get("created_at"),
               updated_at: row.get("updated_at"),
           })
       }

       pub async fn list_buckets(&self) -> Result<Vec<BucketRecord>> {
           let rows = sqlx::query(
               "SELECT id, name, created_at, updated_at FROM buckets ORDER BY name"
           )
           .fetch_all(&self.pool)
           .await?;

           Ok(rows.into_iter().map(|row| BucketRecord {
               id: row.get("id"),
               name: row.get("name"),
               created_at: row.get("created_at"),
               updated_at: row.get("updated_at"),
           }).collect())
       }


       pub async fn delete_bucket(&self, name: &str) -> Result<()> {
              let result = sqlx::query("DELETE FROM buckets WHERE name = ?")
                  .bind(name)
                  .execute(&self.pool)
                  .await?;

              if result.rows_affected() == 0 {
                  return Err(DbError::BucketNotFound(name.to_string()));
              }

              Ok(())
          }


          pub async fn create_object(
                  &self,
                  bucket_id: i64,
                  key: &str,
                  size: i64,
                  content_type: &str,
                  md5_checksum: &str,
                  sha256_checksum: &str,
                  storage_path: &str,
                  custom_metadata: Option<HashMap<String, String>>,
              ) -> Result<ObjectRecord> {
                  let now = Utc::now();

                  let mut tx = self.pool.begin().await?;

                  // Insert or update object
                  let result = sqlx::query(
                      r#"
                      INSERT INTO objects (bucket_id, key, size, content_type, md5_checksum,
                                         sha256_checksum, storage_path, created_at, modified_at)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                      ON CONFLICT(bucket_id, key) DO UPDATE SET
                          size = excluded.size,
                          content_type = excluded.content_type,
                          md5_checksum = excluded.md5_checksum,
                          sha256_checksum = excluded.sha256_checksum,
                          storage_path = excluded.storage_path,
                          modified_at = excluded.modified_at
                      "#
                  )
                  .bind(bucket_id)
                  .bind(key)
                  .bind(size)
                  .bind(content_type)
                  .bind(md5_checksum)
                  .bind(sha256_checksum)
                  .bind(storage_path)
                  .bind(now)
                  .bind(now)
                  .execute(&mut *tx)
                  .await?;

                  let object_id = result.last_insert_rowid();

                  // Insert custom metadata if provided
                  if let Some(metadata) = custom_metadata {
                      // Delete existing metadata
                      sqlx::query("DELETE FROM object_metadata WHERE object_id = ?")
                          .bind(object_id)
                          .execute(&mut *tx)
                          .await?;

                      // Insert new metadata
                      for (k, v) in metadata {
                          sqlx::query("INSERT INTO object_metadata (object_id, key, value) VALUES (?, ?, ?)")
                              .bind(object_id)
                              .bind(&k)
                              .bind(&v)
                              .execute(&mut *tx)
                              .await?;
                      }
                  }

                  tx.commit().await?;

                  Ok(ObjectRecord {
                      id: object_id,
                      bucket_id,
                      key: key.to_string(),
                      size,
                      content_type: content_type.to_string(),
                      md5_checksum: md5_checksum.to_string(),
                      sha256_checksum: sha256_checksum.to_string(),
                      storage_path: storage_path.to_string(),
                      created_at: now,
                      modified_at: now,
                  })
              }


              pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<ObjectRecord> {
                   let row = sqlx::query(
                       r#"
                       SELECT id, bucket_id, key, size, content_type, md5_checksum,
                              sha256_checksum, storage_path, created_at, modified_at
                       FROM objects WHERE bucket_id = ? AND key = ?
                       "#
                   )
                   .bind(bucket_id)
                   .bind(key)
                   .fetch_optional(&self.pool)
                   .await?
                   .ok_or_else(|| DbError::ObjectNotFound(key.to_string()))?;

                   Ok(self.row_to_object_record(row))
               }


               pub async fn get_object_metadata(&self, object_id: i64) -> Result<HashMap<String, String>> {
                      let rows = sqlx::query("SELECT key, value FROM object_metadata WHERE object_id = ?")
                          .bind(object_id)
                          .fetch_all(&self.pool)
                          .await?;

                      Ok(rows.into_iter().map(|row| {
                          (row.get::<String, _>("key"), row.get::<String, _>("value"))
                      }).collect())
                  }



                  pub async fn list_objects(&self, bucket_id: i64, prefix: Option<&str>) -> Result<Vec<ObjectRecord>> {
                         let rows = if let Some(pfx) = prefix {
                             sqlx::query(
                                 r#"
                                 SELECT id, bucket_id, key, size, content_type, md5_checksum,
                                        sha256_checksum, storage_path, created_at, modified_at
                                 FROM objects WHERE bucket_id = ? AND key LIKE ? ORDER BY key
                                 "#
                             )
                             .bind(bucket_id)
                             .bind(format!("{}%", pfx))
                             .fetch_all(&self.pool)
                             .await?
                         } else {
                             sqlx::query(
                                 r#"
                                 SELECT id, bucket_id, key, size, content_type, md5_checksum,
                                        sha256_checksum, storage_path, created_at, modified_at
                                 FROM objects WHERE bucket_id = ? ORDER BY key
                                 "#
                             )
                             .bind(bucket_id)
                             .fetch_all(&self.pool)
                             .await?
                         };

                         Ok(rows.into_iter().map(|row| self.row_to_object_record(row)).collect())
                     }


                     pub async fn delete_object(&self, bucket_id: i64, key: &str) -> Result<()> {
                          let result = sqlx::query("DELETE FROM objects WHERE bucket_id = ? AND key = ?")
                              .bind(bucket_id)
                              .bind(key)
                              .execute(&self.pool)
                              .await?;

                          if result.rows_affected() == 0 {
                              return Err(DbError::ObjectNotFound(key.to_string()));
                          }

                          Ok(())
                      }


                      fn row_to_object_record(&self, row: SqliteRow) -> ObjectRecord {
                          ObjectRecord {
                              id: row.get("id"),
                              bucket_id: row.get("bucket_id"),
                              key: row.get("key"),
                              size: row.get("size"),
                              content_type: row.get("content_type"),
                              md5_checksum: row.get("md5_checksum"),
                              sha256_checksum: row.get("sha256_checksum"),
                              storage_path: row.get("storage_path"),
                              created_at: row.get("created_at"),
                              modified_at: row.get("modified_at"),
                          }
                      }

}
