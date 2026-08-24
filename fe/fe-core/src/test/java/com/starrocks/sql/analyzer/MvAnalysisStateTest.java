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

import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A CREATE with {@code refresh_mode = auto} analyses the statement as INCREMENTAL and, if that is rejected,
 * rolls it back and re-analyses as PCT. A field the rollback misses survives into the PCT pass and corrupts
 * the mv. Grepping for setters does not find every write -- two fields are filled through their getter, one
 * through {@code context.getStatement()} -- so this pins the statement's field set instead.
 */
public class MvAnalysisStateTest {

    /** Written by the CREATE analysis after the refresh mode is chosen. MvAnalysisState must roll each back. */
    private static final Set<String> ANALYSIS_WRITTEN = Set.of(
            "keysType",
            "rowIdStrategy",
            "analyzedRefreshMode",
            "encodeRowIdVersion",
            "baseTableInfos",
            "sortKeys",
            "mvColumnItems",
            "queryOutputIndices",
            "mvIndexes",
            "partitionColumns",
            "partitionRefTableExprs",
            "partitionByExprs",
            "partitionType",
            "isRefBaseTablePartitionWithTransform",
            "properties",
            "generatedPartitionCols",
            "partitionByExprToAdjustExprMap",
            "distributionDesc",
            // Rolled back by re-parsing inlineViewDef: IVMAnalyzer mutates the relation in place, so keeping
            // the old reference is not a rollback.
            "queryStatement");

    /** Owned by the parser, or fixed before the refresh mode is chosen: an AUTO retry cannot dirty them. */
    private static final Set<String> MODE_INDEPENDENT = Set.of(
            "tableRef",
            "colWithComments",
            "indexDefs",
            "ifNotExists",
            "comment",
            "refreshSchemeDesc",
            "queryStartIndex",
            "queryStopIndex",
            "orderByElements",
            "inlineViewDef",
            "simpleViewDef",
            "originalViewDefineSql",
            "ivmViewDef",
            "originalDBName",
            "withRowAccessPolicies");

    @Test
    public void everyStatementFieldIsClassified() {
        List<String> unclassified = Arrays.stream(CreateMaterializedViewStatement.class.getDeclaredFields())
                .filter(field -> !field.isSynthetic() && !Modifier.isStatic(field.getModifiers()))
                .map(Field::getName)
                .filter(name -> !ANALYSIS_WRITTEN.contains(name) && !MODE_INDEPENDENT.contains(name))
                .collect(Collectors.toList());
        assertTrue(unclassified.isEmpty(),
                "New CreateMaterializedViewStatement field(s) " + unclassified + ". If the CREATE analysis "
                        + "writes one after the refresh mode is chosen, roll it back in "
                        + "MaterializedViewAnalyzer.MvAnalysisState and list it in ANALYSIS_WRITTEN; "
                        + "otherwise list it in MODE_INDEPENDENT.");
    }

    @Test
    public void rollbackCoversEveryAnalysisWrittenField() throws Exception {
        Class<?> state = Arrays.stream(MaterializedViewAnalyzer.class.getDeclaredClasses())
                .filter(clazz -> clazz.getSimpleName().equals("MvAnalysisState"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("MvAnalysisState is gone; the AUTO rollback moved"));
        // queryStatement is the one entry the record does not carry -- restore() re-parses it instead.
        assertEquals(ANALYSIS_WRITTEN.size() - 1, state.getRecordComponents().length,
                "MvAnalysisState captures " + state.getRecordComponents().length + " fields but "
                        + (ANALYSIS_WRITTEN.size() - 1) + " are marked analysis-written; a field was added to "
                        + "one and not the other.");
    }
}
