package com.starrocks.storagevolume.credential.s3;

public class AWSDefaultCredential implements AWSCredential {
    @Override
    public AWSCredentialType type() {
        return AWSCredentialType.DEFAULT;
    }
}
