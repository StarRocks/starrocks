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

package com.starrocks.credential.provider;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

// We have to implement DefaultCredentialsProvider by ourselves,
// otherwise we may face "java.lang.IllegalStateException: Connection pool shut down" error.
//
// Hadoop S3AFileSystem will call `static AwsCredentialsProvider::create()` to create CredentialsProvider.
// But in DefaultCredentialsProvider::create(), it will only return a global static variable.
// If we close S3AFileSystem, it will also close CredentialsProvider.
// For the next time we create a new S3AFileSystem, it will reuse previous closed CredentialsProvider, then error will be thrown
// You can check details in link:
// https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/troubleshooting.html#faq-connection-pool-shutdown-exception
public class OverwriteAwsDefaultCredentialsProvider implements AwsCredentialsProvider {
    public static DefaultCredentialsProvider create() {
        return DefaultCredentialsProvider.builder().build();
    }

    // We should not call this function, here will return an anonymous credentials
    @Override
    public AwsCredentials resolveCredentials() {
        // Defense code, return anonymous credentials
        return new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return null;
            }

            @Override
            public String secretAccessKey() {
                return null;
            }
        };
    }
}
