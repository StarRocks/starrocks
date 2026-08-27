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

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StatisticSQLBuilderTest {

    @Test
    void nameBasedDeletesEscapeNames() {
        String sql = StatisticSQLBuilder.buildDropExternalStatSQL("ice'berg", "d'b", "o'brien\\table");
        Assertions.assertTrue(sql.contains("CATALOG_NAME = 'ice''berg'"), sql);
        Assertions.assertTrue(sql.contains("DB_NAME = 'd''b'"), sql);
        Assertions.assertTrue(sql.contains("TABLE_NAME = 'o''brien\\\\table'"), sql);

        String histogramSql = StatisticSQLBuilder.buildDropExternalHistogramSQL("ice'berg", "d'b", "o'brien",
                ImmutableList.of("c'1"));
        Assertions.assertTrue(histogramSql.contains("catalog_name = 'ice''berg'"), histogramSql);
        Assertions.assertTrue(histogramSql.contains("db_name = 'd''b'"), histogramSql);
        Assertions.assertTrue(histogramSql.contains("table_name = 'o''brien'"), histogramSql);
        Assertions.assertTrue(histogramSql.contains("column_name in ('c''1')"), histogramSql);
    }
}
