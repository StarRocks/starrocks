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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.MaterializedViewOptimizer;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class MaterializedViewAnalyzerWithPaimonTest {
    static StarRocksAssert starRocksAssert;
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        ConnectorPlanTestBase.mockAllCatalogs(starRocksAssert.getCtx(), temp.toURI().toString());

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(starRocksAssert.getCtx());
    }

    @AfterAll
    public static void afterClass() {
        ConnectorPlanTestBase.dropAllCatalogs();
    }

    @Test
    public void testMaterializedAnalyPaimonTable(@Mocked SlotRef slotRef, @Mocked PaimonTable table) {
        MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor materializedViewAnalyzerVisitor =
                new MaterializedViewAnalyzer.MaterializedViewAnalyzerVisitor();

        new MockUp<MaterializedViewOptimizer>() {
            @Mock
            public MvPlanContext optimize(MaterializedView mv,
                                          ConnectContext connectContext,
                                          boolean inlineView,
                                          boolean isCheckNonDeterministicFunction) {
                return new MvPlanContext(false, "");
            }
        };

        {
            // test check partition column can not be found
            boolean checkSuccess = false;
            new Expectations() {
                {
                    table.isUnPartitioned();
                    result = false;
                }
            };
            try {
                materializedViewAnalyzerVisitor.checkPartitionColumnWithBasePaimonTable(slotRef, table);
                checkSuccess = true;
            } catch (Exception e) {
                Assertions.assertTrue(e.getMessage().contains("Materialized view partition column in partition exp " +
                                "must be base table partition column"),
                        e.getMessage());
            }
            Assertions.assertFalse(checkSuccess);
        }

        {
            // test check successfully
            boolean checkSuccess = false;
            new Expectations() {
                {
                    table.isUnPartitioned();
                    result = false;

                    table.getPartitionColumnNames();
                    result = Lists.newArrayList("dt");

                    slotRef.getColumnName();
                    result = "dt";

                    table.getColumn("dt");
                    result = new Column("dt", ScalarType.createType(PrimitiveType.DATE));
                }
            };
            try {
                materializedViewAnalyzerVisitor.checkPartitionColumnWithBasePaimonTable(slotRef, table);
                checkSuccess = true;
            } catch (Exception e) {
            }
            Assertions.assertTrue(checkSuccess);
        }

        {
            //test paimon table is unparitioned
            new Expectations() {
                {
                    table.isUnPartitioned();
                    result = true;
                }
            };

            boolean checkSuccess = false;
            try {
                materializedViewAnalyzerVisitor.checkPartitionColumnWithBasePaimonTable(slotRef, table);
            } catch (Exception e) {
                Assertions.assertTrue(e.getMessage().contains("Materialized view partition column in partition exp " +
                                "must be base table partition column"),
                        e.getMessage());
            }
            Assertions.assertFalse(checkSuccess);
        }
    }
}
