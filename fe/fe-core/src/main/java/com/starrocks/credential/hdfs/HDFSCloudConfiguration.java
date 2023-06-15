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

package com.starrocks.credential.hdfs;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.staros.proto.FileStoreInfo;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudType;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;

public class HDFSCloudConfiguration implements CloudConfiguration {
    private final HDFSCloudCredential hdfsCloudCredential;

    public HDFSCloudConfiguration(HDFSCloudCredential hdfsCloudCredential) {
        Preconditions.checkNotNull(hdfsCloudCredential);
        this.hdfsCloudCredential = hdfsCloudCredential;
    }

    public HDFSCloudCredential getHdfsCloudCredential() {
        return hdfsCloudCredential;
    }

    @Override
    public void toThrift(TCloudConfiguration tCloudConfiguration) {
        // TODO
    }

    @Override
    public void applyToConfiguration(Configuration configuration) {
        // TODO
    }

    @Override
    public String getCredentialString() {
        return hdfsCloudCredential.getCredentialString();
    }

    @Override
    public CloudType getCloudType() {
        return CloudType.HDFS;
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        return hdfsCloudCredential.toFileStoreInfo();
    }
}
