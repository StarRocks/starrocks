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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnEntry;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public interface RowInfo {

    List<ColumnEntry> getColumnEntries();

    Map<ColumnRefOperator, ScalarOperator> getColumnRefMap();

    ColumnRefSet getOutputColumnRefSet();

    ColumnRefSet getUsedColumnRefSet();

    int getColumnCount();

    ColumnEntry getColumnEntry(ColumnRefOperator columnRefOperator);

    ColumnEntry rewriteColWithRowInfo(ColumnEntry columnEntry);

    RowInfo mergeRowInfo(RowInfo rowInfo);

    // project the rowInfo with other columnEntries.
    // If existProjection is true, it means we need rewrite scalaOperator with exist rowInfo
    // then add these columnEntries to the original projection.
    // If existProjection is false, it means we need add a new projection to this exist rowInfo
    RowInfo addColsToRow(List<ColumnEntry> entryList, boolean existProjection);

}
