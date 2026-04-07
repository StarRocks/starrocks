// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common.udf;

import com.starrocks.common.udf.impl.DefaultStorageHandler;
import com.starrocks.common.udf.impl.S3StorageHandler;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;

public class StorageHandlerFactory {
    public static StorageHandler create(CloudConfiguration cloudConfiguration) {
        if (cloudConfiguration == null || cloudConfiguration.getCloudType() == CloudType.DEFAULT) {
            return new DefaultStorageHandler();
        }
        switch (cloudConfiguration.getCloudType()) {
            case AWS:
                return new S3StorageHandler(cloudConfiguration);
            default:
                String errMsg = String.format("%s Cloud type is not supported", cloudConfiguration.getCloudType());
                throw new UnsupportedOperationException(errMsg);
        }
    }
}