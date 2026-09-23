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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ProcUtilsTest {

    @Test
    public void testAnalyzeColumnResolvesAgainstTheListItIsGiven() throws AnalysisException {
        // The layouts disagree on where a column sits, which is the whole reason the list is a
        // parameter: State is index 9 for schema changes and 8 for rollups.
        Assertions.assertEquals(9, ProcUtils.analyzeColumn(SchemaChangeProcDir.TITLE_NAMES, "State"));
        Assertions.assertEquals(8, ProcUtils.analyzeColumn(RollupProcDir.TITLE_NAMES, "State"));
        Assertions.assertEquals(6, ProcUtils.analyzeColumn(OptimizeProcDir.TITLE_NAMES, "State"));
    }

    @Test
    public void testAnalyzeColumnIgnoresCase() throws AnalysisException {
        List<String> titleNames = List.of("JobId", "TableName", "CreateTime");
        Assertions.assertEquals(1, ProcUtils.analyzeColumn(titleNames, "TableName"));
        Assertions.assertEquals(1, ProcUtils.analyzeColumn(titleNames, "tablename"));
        Assertions.assertEquals(1, ProcUtils.analyzeColumn(titleNames, "TABLENAME"));
    }

    @Test
    public void testAnalyzeColumnRejectsAnUnknownName() {
        List<String> titleNames = List.of("JobId", "TableName");
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> ProcUtils.analyzeColumn(titleNames, "NoSuchColumn"));
        Assertions.assertTrue(e.getMessage().contains("Title name[NoSuchColumn] does not exist"),
                e.getMessage());
    }

    @Test
    public void testAnalyzeColumnOnAnEmptyList() {
        Assertions.assertThrows(AnalysisException.class, () -> ProcUtils.analyzeColumn(List.of(), "JobId"));
    }
}
