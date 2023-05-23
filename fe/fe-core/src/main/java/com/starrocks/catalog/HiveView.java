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

import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HiveView extends View {
    private final String catalogName;
    public HiveView(long id, String catalogName, String name, List<Column> schema, String definition) {
        super(id, name, schema);
        this.catalogName = requireNonNull(catalogName, "Hive view catalog name is null");
        this.inlineViewDef = requireNonNull(definition, "Hive view text is null");
    }

    @Override
    public QueryStatement getQueryStatement() throws StarRocksPlannerException {
        QueryStatement queryStatement = super.getQueryStatement();
        List<TableRelation> tableRelations = AnalyzerUtils.collectTableRelations(queryStatement);
        for (TableRelation tableRelation : tableRelations) {
            tableRelation.getName().setCatalog(catalogName);
        }
        return queryStatement;
    }


}
