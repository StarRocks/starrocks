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

package com.starrocks.datacache.copilot;

import com.starrocks.analysis.LimitElement;
import com.starrocks.sql.ast.QualifiedName;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class DataCacheCopilotSimpleRecommenderTest {

    @Test
    public void testCollectSQLBuilder() {
        String sql =
                DataCacheCopilotSimpleRecommender.CollectSQLBuilder.build(Optional.empty(), 0, new LimitElement(100));
        Assert.assertEquals(
                "SELECT CAST(20 AS INT), `catalog_name`, `database_name`, `table_name`, " +
                        "GROUP_CONCAT(`partition_name` SEPARATOR ',') AS `partition_names`, " +
                        "GROUP_CONCAT(`column_name` SEPARATOR ',') AS `column_names`, `count` " +
                        "FROM `default_catalog`.`_statistics_`.`datacache_copilot_statistics`  " +
                        "GROUP BY `catalog_name`, `database_name`, `table_name`, `access_time`, `count` " +
                        "ORDER BY `count` DESC  LIMIT 100",
                sql);

        // test with different QualifiedName
        QualifiedName qualifiedNameWithCatalog = QualifiedName.of(List.of("catalog_name"));
        QualifiedName qualifiedNameWithDb = QualifiedName.of(List.of("catalog_name", "database_name"));
        QualifiedName qualifiedNameWithTable = QualifiedName.of(List.of("catalog_name", "database_name", "table_name"));

        sql = DataCacheCopilotSimpleRecommender.CollectSQLBuilder.build(Optional.of(qualifiedNameWithCatalog), 0,
                new LimitElement(100));
        Assert.assertTrue(sql.contains("WHERE `catalog_name` = 'catalog_name'"));

        sql = DataCacheCopilotSimpleRecommender.CollectSQLBuilder.build(Optional.of(qualifiedNameWithDb), 0,
                new LimitElement(100));
        Assert.assertTrue(sql.contains("WHERE `catalog_name` = 'catalog_name' AND `database_name` = 'database_name'"));

        sql = DataCacheCopilotSimpleRecommender.CollectSQLBuilder.build(Optional.of(qualifiedNameWithTable), 0,
                new LimitElement(100));
        Assert.assertTrue(sql.contains(
                "WHERE `catalog_name` = 'catalog_name' AND `database_name` = 'database_name' AND `table_name` = 'table_name'"));

    }
}
