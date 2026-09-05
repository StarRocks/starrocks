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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.common.util.PropertyAnalyzer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class TablePropertyCopyTest {
    @Test
    public void testCopyShouldCopyMutableContainers() {
        Map<String, String> properties = new HashMap<>();
        properties.put("k1", "v1");
        properties.put(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION, "rack:r1,rack:r2");
        properties.put(TableProperty.BINLOG_PARTITION + "1", "10");

        TableProperty original = new TableProperty(properties);
        original.buildLocation();
        original.buildBinlogAvailableVersion();
        original.setFlatJsonConfig(new FlatJsonConfig(true, 0.1, 0.8, 50));
        original.setBinlogConfig(new BinlogConfig(1, true, 60, 100));
        original.setExcludedTriggerTables(Lists.newArrayList(new TableName("ext_catalog", "db1", "t1")));
        original.setMvSortKeys(Lists.newArrayList("k1", "k2"));
        original.setUniqueConstraints(Lists.newArrayList(new UniqueConstraint("ext_catalog", "db1", "t1",
                Lists.newArrayList(ColumnId.create("c1")))));
        original.setForeignKeyConstraints(Lists.newArrayList(new ForeignKeyConstraint(
                new BaseTableInfo("ext_catalog", "db1", "parent_table", "parent_identifier"),
                new BaseTableInfo("ext_catalog", "db1", "child_table", "child_identifier"),
                Lists.newArrayList(Pair.create(ColumnId.create("child_col"), ColumnId.create("parent_col"))))));
        original.setHasDelete(true);
        original.setHasForbiddenGlobalDict(true);

        TableProperty copied = original.copy();

        Assertions.assertNotSame(original, copied);
        Assertions.assertNotSame(original.getProperties(), copied.getProperties());
        Assertions.assertNotSame(original.getFlatJsonConfig(), copied.getFlatJsonConfig());
        Assertions.assertNotSame(original.getBinlogConfig(), copied.getBinlogConfig());
        Assertions.assertNotSame(original.getExcludedTriggerTables(), copied.getExcludedTriggerTables());
        Assertions.assertSame(original.getExcludedTriggerTables().get(0), copied.getExcludedTriggerTables().get(0));
        Assertions.assertNotSame(original.getMvSortKeys(), copied.getMvSortKeys());
        Assertions.assertNotSame(original.getUniqueConstraints(), copied.getUniqueConstraints());
        Assertions.assertNotSame(original.getForeignKeyConstraints(), copied.getForeignKeyConstraints());
        Assertions.assertSame(original.getUniqueConstraints().get(0), copied.getUniqueConstraints().get(0));
        Assertions.assertSame(original.getForeignKeyConstraints().get(0), copied.getForeignKeyConstraints().get(0));
        Assertions.assertNotSame(original.getBinlogAvailableVersions(), copied.getBinlogAvailableVersions());
        Assertions.assertNotSame(original.getLocation(), copied.getLocation());

        original.getProperties().put("k1", "changed");
        original.getFlatJsonConfig().setFlatJsonEnable(false);
        original.getBinlogConfig().setBinlogEnable(false);
        original.getExcludedTriggerTables().add(new TableName("ext_catalog", "db2", "t2"));
        original.getMvSortKeys().add("k3");
        original.getUniqueConstraints().clear();
        original.getForeignKeyConstraints().clear();
        original.getBinlogAvailableVersions().put(2L, 20L);
        original.getLocation().put("rack", "r3");
        original.setHasDelete(false);
        original.setHasForbiddenGlobalDict(false);

        Assertions.assertEquals("v1", copied.getProperties().get("k1"));
        Assertions.assertTrue(copied.getFlatJsonConfig().getFlatJsonEnable());
        Assertions.assertTrue(copied.getBinlogConfig().getBinlogEnable());
        Assertions.assertEquals(1, copied.getExcludedTriggerTables().size());
        Assertions.assertEquals("t1", copied.getExcludedTriggerTables().get(0).getTbl());
        Assertions.assertEquals(2, copied.getMvSortKeys().size());
        Assertions.assertEquals(1, copied.getUniqueConstraints().size());
        Assertions.assertEquals(1, copied.getForeignKeyConstraints().size());
        Assertions.assertEquals(1, copied.getBinlogAvailableVersions().size());
        Assertions.assertFalse(copied.getBinlogAvailableVersions().containsKey(2L));
        Assertions.assertFalse(copied.getLocation().containsEntry("rack", "r3"));
        Assertions.assertTrue(copied.hasDelete());
        Assertions.assertTrue(copied.hasForbiddenGlobalDict());
    }
}
