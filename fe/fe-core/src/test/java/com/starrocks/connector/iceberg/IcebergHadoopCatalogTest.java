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

import com.starrocks.connector.iceberg.hadoop.IcebergHadoopCatalog;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergHadoopCatalogTest {

    @Test
    public void testListAllDatabases(@Mocked IcebergHadoopCatalog hadoopCatalog) {
        new Expectations() {
            {
                hadoopCatalog.listAllDatabases();
                result = Arrays.asList("db1", "db2");
                minTimes = 0;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("iceberg.catalog.warehouse", "s3://path/to/warehouse");
        IcebergHadoopCatalog icebergHadoopCatalog = new IcebergHadoopCatalog(
                "hadoop_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergHadoopCatalog.listAllDatabases();
        Assert.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }
}
