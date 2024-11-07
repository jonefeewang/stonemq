pub struct OffsetMetadata {
    pub offset: i64,
    pub metadata: String,
}

pub struct OffsetAndMetadata {
    pub offset_metadata: OffsetMetadata,
    pub commit_timestamp: i64,
    pub expire_timestamp: i64,
}
