package com.starrocks.common.udf;

import com.starrocks.common.udf.impl.S3StorageHandler;
import com.starrocks.credential.CloudType;
import com.starrocks.storagevolume.StorageVolume;

public class StorageHandlerFactory {
    public static StorageHandler create(StorageVolume sv) {
        CloudType cloudType = sv.getCloudConfiguration().getCloudType();
        switch (cloudType) {
            case AWS:
                return new S3StorageHandler(sv);
            default:
                String errMsg = String.format("%s Cloud type is not supported", sv.getCloudConfiguration().getCloudType());
                throw new UnsupportedOperationException(errMsg);
        }
    }
}