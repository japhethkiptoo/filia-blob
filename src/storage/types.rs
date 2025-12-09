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
