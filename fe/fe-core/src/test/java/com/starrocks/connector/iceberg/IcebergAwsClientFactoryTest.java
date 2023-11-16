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

package com.starrocks.connector.iceberg;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class IcebergAwsClientFactoryTest {

    @Test
    public void testInvalidCredential() {
        IcebergAwsClientFactory factory = new IcebergAwsClientFactory();
        Map<String, String> properties = new HashMap<>();
        factory.initialize(properties);
        Assert.assertThrows(IllegalArgumentException.class, factory::s3);
        Assert.assertThrows(IllegalArgumentException.class, factory::glue);
    }

    @Test
    public void testEnsureSchemeInEndpoint() {
        // test endpoint without scheme
        URI uriWithoutScheme = IcebergAwsClientFactory.ensureSchemeInEndpoint("s3.aa-bbbbb-3.amazonaws.com.cn");
        Assert.assertEquals("https://s3.aa-bbbbb-3.amazonaws.com.cn", uriWithoutScheme.toString());

        // test endpoint with scheme HTTPS
        URI uriWithHttps = IcebergAwsClientFactory.ensureSchemeInEndpoint("https://s3.aa-bbbbb-3.amazonaws.com.cn");
        Assert.assertEquals("https://s3.aa-bbbbb-3.amazonaws.com.cn", uriWithHttps.toString());

        // test endpoint with scheme HTTP
        URI uriWithHttp = IcebergAwsClientFactory.ensureSchemeInEndpoint("http://s3.aa-bbbbb-3.amazonaws.com.cn");
        Assert.assertEquals("http://s3.aa-bbbbb-3.amazonaws.com.cn", uriWithHttp.toString());
    }
}
