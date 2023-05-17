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

package com.starrocks.credential;

import com.staros.proto.FileStoreInfo;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.conf.Configuration;

public interface CloudConfiguration {

    void toThrift(TCloudConfiguration tCloudConfiguration);

    void applyToConfiguration(Configuration configuration);

    // Hadoop FileSystem has a cache itself, it used request uri as a cache key by default,
    // so it cannot sense the CloudCredential changed.
    // So we need to generate an identifier for different CloudCredential, and used it as cache key.
    // getCredentialString() Method just like toString()
    String getCredentialString();

    CloudType getCloudType();

    // Convert to the protobuf used by staros.
    FileStoreInfo toFileStoreInfo();
}
