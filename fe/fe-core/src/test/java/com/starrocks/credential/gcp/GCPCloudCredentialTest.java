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

package com.starrocks.credential.gcp;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GCPCloudCredentialTest {

    @Test
    public void testServiceAccountCredentials() {
        GCPCloudCredential credential = new GCPCloudCredential(
                "", false,
                "test@project.iam.gserviceaccount.com",
                "key-id-123",
                "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
                "", null, null);

        Configuration conf = new Configuration();
        credential.applyToConfiguration(conf);

        assertEquals("SERVICE_ACCOUNT_JSON_KEYFILE", conf.get("fs.gs.auth.type"));
        assertEquals("test@project.iam.gserviceaccount.com",
                conf.get("fs.gs.auth.service.account.email"));
        assertEquals("key-id-123",
                conf.get("fs.gs.auth.service.account.private.key.id"));
        assertEquals("-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
                conf.get("fs.gs.auth.service.account.private.key"));
        assertTrue(credential.validate());
    }

    @Test
    public void testEmptyServiceAccountCredentials() {
        GCPCloudCredential credential = new GCPCloudCredential(
                "", false,
                "", "", "",
                "",
                "ya29.access-token", "2026-12-31T00:00:00Z");

        Configuration conf = new Configuration();
        credential.applyToConfiguration(conf);

        assertNull(conf.get("fs.gs.auth.type"));
        assertNull(conf.get("fs.gs.auth.service.account.email"));
        assertEquals("ya29.access-token",
                conf.get(GCPCloudConfigurationProvider.ACCESS_TOKEN_KEY));
        assertTrue(credential.validate());
    }

    @Test
    public void testComputeEngineServiceAccount() {
        GCPCloudCredential credential = new GCPCloudCredential(
                "", true,
                "", "", "",
                "", null, null);

        Configuration conf = new Configuration();
        credential.applyToConfiguration(conf);

        assertEquals("COMPUTE_ENGINE", conf.get("fs.gs.auth.type"));
        assertNull(conf.get("fs.gs.auth.service.account.email"));
        assertTrue(credential.validate());
    }
}
