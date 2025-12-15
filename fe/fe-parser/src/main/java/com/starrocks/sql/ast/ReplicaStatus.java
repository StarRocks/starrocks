package com.starrocks.sql.ast;

public enum ReplicaStatus {
    OK, // health
    DEAD, // backend is not available
    VERSION_ERROR, // missing version
    MISSING, // replica does not exist
    SCHEMA_ERROR, // replica's schema hash does not equal to index's schema hash
    BAD // replica is broken.
}
