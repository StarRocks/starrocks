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

import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.PartitionField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;

public class IcebergPartitionUtilsTest extends TableTestBase {
    @TempDir
    public static File temp;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());
    }

    @Test
    public void testNormalizeTimePartitionName() {
        new MockUp<TimeUtils>() {
            @Mock
            public  TimeZone getTimeZone() {
                return TimeZone.getTimeZone("GMT+6");
            }
        };
        // year
        // with time zone
        String partitionName = "2020";
        PartitionField partitionField = SPEC_D_2.fields().get(0);
        String result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assertions.assertEquals("2020-01-01 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assertions.assertEquals("2020-01-01", result);
        // without time zone
        partitionField = SPEC_E_2.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assertions.assertEquals("2020-01-01 00:00:00", result);

        // month
        // with time zone
        partitionName = "2020-02";
        partitionField = SPEC_D_3.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assertions.assertEquals("2020-02-01 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assertions.assertEquals("2020-02-01", result);
        // without time zone
        partitionField = SPEC_E_3.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assertions.assertEquals("2020-02-01 00:00:00", result);

        // day
        // with time zone
        partitionName = "2020-01-02";
        partitionField = SPEC_D_4.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assertions.assertEquals("2020-01-02 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assertions.assertEquals("2020-01-02", result);
        // without time zone
        partitionField = SPEC_E_4.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assertions.assertEquals("2020-01-02 00:00:00", result);

        // hour
        partitionName = "2020-01-02-12";
        partitionField = SPEC_D_5.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assertions.assertEquals("2020-01-02 18:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assertions.assertEquals("2020-01-02", result);
        // without time zone
        partitionField = SPEC_E_5.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assertions.assertEquals("2020-01-02 12:00:00", result);
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }
}
