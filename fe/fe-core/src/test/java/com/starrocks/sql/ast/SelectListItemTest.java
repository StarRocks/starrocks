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

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SelectListItemTest {

    @Test
    public void testCopyConstructorDeepCopyExcludedColumns() {
        List<String> originalExcluded = new ArrayList<>(Arrays.asList("col1", "col2"));
        SelectListItem original = new SelectListItem(
                new TableName("db", "tbl"), 
                NodePosition.ZERO, 
                originalExcluded
        );

        SelectListItem copied = new SelectListItem(original);

        originalExcluded.add("col3");
        original.getExcludedColumns().add("col4");

        Assert.assertNotSame("excludedColumns should be a deep copy, not a shared reference",
                original.getExcludedColumns(),
                copied.getExcludedColumns());
        
        Assert.assertEquals("excludedColumns should be same to before",
                Arrays.asList("col1", "col2"),
                copied.getExcludedColumns());
    }

    @Test
    public void testCopyConstructorWithEmptyExcludedColumns() {
        List<String> originalExcluded = new ArrayList<>();
        SelectListItem original = new SelectListItem(
                new TableName("db", "tbl"), 
                NodePosition.ZERO, 
                originalExcluded
        );

        SelectListItem copied = new SelectListItem(original);

        originalExcluded.add("new_col");
        Assert.assertEquals("Empty lists should remain empty after deep copying", 
                Collections.emptyList(), 
                copied.getExcludedColumns());
    }
}