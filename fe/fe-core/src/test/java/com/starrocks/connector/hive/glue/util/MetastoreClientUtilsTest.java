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

package com.starrocks.connector.hive.glue.util;

import com.starrocks.connector.hive.glue.metastore.GlueMetastoreClientDelegate;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class MetastoreClientUtilsTest {

    @Test
    public void testGetGlueCatalogId() {
        Configuration conf = new Configuration();
        Assert.assertNull(MetastoreClientUtils.getCatalogId(conf));
        conf.set(GlueMetastoreClientDelegate.CATALOG_ID_CONF, "123");
        Assert.assertEquals("123", MetastoreClientUtils.getCatalogId(conf));
        conf = new Configuration();
        conf.set(CloudConfigurationConstants.AWS_GLUE_CATALOG_ID, "1234");
        Assert.assertEquals("1234", MetastoreClientUtils.getCatalogId(conf));
    }
}
