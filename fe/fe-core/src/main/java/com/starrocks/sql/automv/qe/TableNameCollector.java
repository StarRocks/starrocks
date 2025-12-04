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

package com.starrocks.sql.automv.qe;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.TableRelation;

import java.util.List;

public class TableNameCollector implements AopAstHandler {
    private final List<TableName> tableNames = Lists.newArrayList();

    public void preProcess(Object node) {
        Preconditions.checkArgument(node instanceof ParseNode);
        if (!(node instanceof TableRelation)) {
            return;
        }
        TableRelation tableRel = (TableRelation) node;
        tableNames.add(tableRel.getName());
    }

    @Override
    public void postProcess(Object object) {

    }

    public List<TableName> getTableNames() {
        return ImmutableList.copyOf(tableNames);
    }
}
