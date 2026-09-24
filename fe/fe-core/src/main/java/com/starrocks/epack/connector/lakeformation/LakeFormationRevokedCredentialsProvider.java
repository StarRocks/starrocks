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

package com.starrocks.epack.connector.lakeformation;

import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.io.IOException;
import java.net.URI;

/**
 * The credential provider a released Delta reader leaves in its configuration: constructing it fails, so
 * no file system can be built from that configuration again.
 *
 * <p>A released reader's configuration can still build file systems lazily; this provider makes that lookup fail.
 *
 * <p>Replaced rather than unset: unsetting would fall back to the catalog's or the SDK's ambient identity.
 */
public final class LakeFormationRevokedCredentialsProvider implements AwsCredentialsProvider {

    /**
     * The only constructor on purpose. S3A chooses among several signatures by reflection, so a second one
     * that succeeded would hand back a usable file system after the credentials were released.
     */
    public LakeFormationRevokedCredentialsProvider(URI uri, Configuration configuration) throws IOException {
        throw new IOException("The Lake Formation credentials this file system was built with were released"
                + " when the query that vended them finished, so it cannot be opened again.");
    }

    /** Unreachable: the constructor above always throws. Present because the interface requires it. */
    @Override
    public AwsCredentials resolveCredentials() {
        throw new IllegalStateException("A revoked Lake Formation credential provider cannot be constructed.");
    }
}
