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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// FOREIGN KEY: (column1, column2) REFERENCES catalog.database.table(column1', column2')
// if catalog or database is not provided, use current catalog or current database instead.
public class ForeignKeyConstraint {
    private static final String FOREIGN_KEY_REGEX = "^\\(([a-zA-Z]\\w{0,63}|_[a-zA-Z0-9]\\w{0,62})" +
            "(,(\\s*[a-zA-Z]\\w{0,63}\\s*)|,(\\s*_[a-zA-Z0-9]\\w{0,62}\\s*))*\\)$";
    public static final Pattern FOREIGN_KEY_PATTERN = Pattern.compile(FOREIGN_KEY_REGEX);
    // table with primary key or unique key
    // if parent table is dropped, the foreign key is not dropped cascade now.
    private BaseTableInfo parentTableInfo;

    // here id is preferred, but meta of column does not have id.
    // have to use name here, so column rename is not supported
    // eg: [column1 -> column1', column2 -> column2']
    private List<Pair<String, String>> columnRefPairs;

    private String constraintStr;

    public ForeignKeyConstraint(
            BaseTableInfo parentTableInfo,
            List<Pair<String, String>> columnRefPairs) {
        this.parentTableInfo = parentTableInfo;
        this.columnRefPairs = columnRefPairs;
    }

    public BaseTableInfo getParentTableInfo() {
        return parentTableInfo;
    }

    public List<Pair<String, String>> getColumnRefPairs() {
        return columnRefPairs;
    }

    // for default catalog, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for other catalog, the format is: (column1, column2) REFERENCES default_catalog.dbname.tablename(column1', column2')
    @Override
    public String toString() {
        if (parentTableInfo == null || columnRefPairs == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        String baseColumns = Joiner.on(".").join(columnRefPairs.stream().map(pair -> pair.first).collect(Collectors.toList()));
        sb.append(baseColumns);
        sb.append(")");
        sb.append(" REFERENCES ");
        sb.append(parentTableInfo.toString());
        sb.append("(");
        String parentColumns = Joiner.on(".").join(columnRefPairs.stream().map(pair -> pair.second).collect(Collectors.toList()));
        sb.append(parentColumns);
        sb.append(")");
        return sb.toString();
    }

    public String getConstraintStr() {
        return constraintStr;
    }

    // for default catalog, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for other catalog, the format is: (column1, column2) REFERENCES default_catalog.dbname.tablename(column1', column2')
    public static List<ForeignKeyConstraint> parse(String foreignKeyConstraintStr) {
        // TODO: use regex to parse the infos
        Matcher foreignKeyMatcher = FOREIGN_KEY_PATTERN.matcher(foreignKeyConstraintStr);
        List<ForeignKeyConstraint> foreignKeyConstraints = Lists.newArrayList();
        List<String> splits = Lists.newArrayList();
        for (String split : splits) {
            String catalogName = "";
            String db = "";
            String table = "";
            List<Pair<String, String>> columnRefPairs = Lists.newArrayList();
            if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                BaseTableInfo parentTableInfo = new BaseTableInfo(Long.parseLong(db), Long.parseLong(table));
                foreignKeyConstraints.add(new ForeignKeyConstraint(parentTableInfo, columnRefPairs));
            } else {
                BaseTableInfo parentTableInfo = new BaseTableInfo(catalogName, db, table);
                foreignKeyConstraints.add(new ForeignKeyConstraint(parentTableInfo, columnRefPairs));
            }
        }
        return foreignKeyConstraints;
    }
}
