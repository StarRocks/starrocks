package com.starrocks.storagevolume.credential.s3;

public interface AWSCredential {
    public AWSCredential type();

    public enum AWSCredentialType {
        DEFAULT,
        SIMPLE,
        IAM,
        INSTANCE_PROFILE,
        ASSUME_ROLE
    }
}
