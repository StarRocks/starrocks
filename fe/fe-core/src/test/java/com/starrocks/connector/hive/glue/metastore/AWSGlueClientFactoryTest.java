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

package com.starrocks.connector.hive.glue.metastore;


import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;


public class AWSGlueClientFactoryTest {

    @Test
    public void testNewClientWithEndpointWithoutScheme() throws MetaException {
        // Test line 53: ensureSchemeInEndpoint is called when endpoint is set
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("aws.glue.access_key", "ak");
        hiveConf.set("aws.glue.secret_key", "sk");
        hiveConf.set("aws.glue.region", "us-west-1");
        hiveConf.set("aws.glue.endpoint", "localhost:4566");

        AWSGlueClientFactory factory = new AWSGlueClientFactory(hiveConf);
        GlueClient client = factory.newClient();
        Assertions.assertNotNull(client);

        // Verify that endpointOverride was set with https:// prefix
        // We can't directly access the endpointOverride, but we can verify
        // that the client was created successfully with the endpoint
        client.close();
    }

    @Test
    public void testNewClientWithEndpointWithScheme() throws MetaException {
        // Test line 53: ensureSchemeInEndpoint preserves existing scheme
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("aws.glue.access_key", "ak");
        hiveConf.set("aws.glue.secret_key", "sk");
        hiveConf.set("aws.glue.region", "us-west-1");
        hiveConf.set("aws.glue.endpoint", "https://glue.us-west-1.amazonaws.com");

        AWSGlueClientFactory factory = new AWSGlueClientFactory(hiveConf);
        GlueClient client = factory.newClient();
        Assertions.assertNotNull(client);
        client.close();
    }
}

