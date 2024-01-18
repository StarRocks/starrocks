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
import com.google.common.base.Preconditions;
<<<<<<< HEAD
=======
import com.google.common.base.Strings;
>>>>>>> 2.5.18
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.starrocks.common.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
<<<<<<< HEAD
import org.spark_project.guava.base.Strings;
=======
>>>>>>> 2.5.18

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// foreign key constraint is used to guide optimizer rewrite for now,
// and is not enforced during ingestion.
// the foreign key property of data should be guaranteed by user.
//
// a table may have multi foreign key constraints.
public class ForeignKeyConstraint {
    private static final Logger LOG = LogManager.getLogger(ForeignKeyConstraint.class);
<<<<<<< HEAD

    private static final String FOREIGN_KEY_REGEX = "\\(((,?\\s*\\w+\\s*)+)\\)\\s+((?i)REFERENCES)\\s+" +
            "((\\.?\\w+)+)\\s*\\(((,?\\s*\\w+\\s*)+)\\)";
    public static final Pattern FOREIGN_KEY_PATTERN = Pattern.compile(FOREIGN_KEY_REGEX);
=======
    private static final String FOREIGN_KEY_REGEX = "((\\.?\\w+:?)*)\\s*\\(((,?\\s*\\w+\\s*)+)\\)\\s+((?i)REFERENCES)\\s+" +
            "((\\.?\\w+:?)+)\\s*\\(((,?\\s*\\w+\\s*)+)\\)";

    public static final Pattern FOREIGN_KEY_PATTERN = Pattern.compile(FOREIGN_KEY_REGEX);

>>>>>>> 2.5.18
    // table with primary key or unique key
    // if parent table is dropped, the foreign key is not dropped cascade now.
    private final BaseTableInfo parentTableInfo;

<<<<<<< HEAD
=======
    // table with foreign key, it only used for materialized view.
    private final BaseTableInfo childTableInfo;

>>>>>>> 2.5.18
    // here id is preferred, but meta of column does not have id.
    // have to use name here, so column rename is not supported
    // eg: [column1 -> column1', column2 -> column2']
    private final List<Pair<String, String>> columnRefPairs;

    public ForeignKeyConstraint(
            BaseTableInfo parentTableInfo,
<<<<<<< HEAD
            List<Pair<String, String>> columnRefPairs) {
        this.parentTableInfo = parentTableInfo;
=======
            BaseTableInfo childTableInfo,
            List<Pair<String, String>> columnRefPairs) {
        this.parentTableInfo = parentTableInfo;
        this.childTableInfo = childTableInfo;
>>>>>>> 2.5.18
        this.columnRefPairs = columnRefPairs;
    }

    public BaseTableInfo getParentTableInfo() {
        return parentTableInfo;
    }

<<<<<<< HEAD
=======
    public BaseTableInfo getChildTableInfo() {
        return childTableInfo;
    }

>>>>>>> 2.5.18
    public List<Pair<String, String>> getColumnRefPairs() {
        return columnRefPairs;
    }

<<<<<<< HEAD
    // for default catalog, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for other catalog, the format is: (column1, column2) REFERENCES default_catalog.dbname.tablename(column1', column2')
=======
    // for olap table, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for materialized view, the format is: catalog1.dbName1.tableName1(column1, column2) REFERENCES
    // catalog2.dbName2.tableName2(column1', column2')
>>>>>>> 2.5.18
    @Override
    public String toString() {
        if (parentTableInfo == null || columnRefPairs == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
<<<<<<< HEAD
=======
        if (childTableInfo != null) {
            sb.append(childTableInfo);
        }
>>>>>>> 2.5.18
        sb.append("(");
        String baseColumns = Joiner.on(",").join(columnRefPairs.stream().map(pair -> pair.first).collect(Collectors.toList()));
        sb.append(baseColumns);
        sb.append(")");
        sb.append(" REFERENCES ");
<<<<<<< HEAD
        sb.append(parentTableInfo.toString());
=======
        sb.append(parentTableInfo);
>>>>>>> 2.5.18
        sb.append("(");
        String parentColumns = Joiner.on(",").join(columnRefPairs.stream().map(pair -> pair.second).collect(Collectors.toList()));
        sb.append(parentColumns);
        sb.append(")");
        return sb.toString();
    }

    // for default catalog, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for other catalog, the format is: (column1, column2) REFERENCES default_catalog.dbname.tablename(column1', column2')
    public static List<ForeignKeyConstraint> parse(String foreignKeyConstraintDescs) {
        if (Strings.isNullOrEmpty(foreignKeyConstraintDescs)) {
            return null;
        }
        String[] constraintArray = foreignKeyConstraintDescs.split(";");
        List<ForeignKeyConstraint> foreignKeyConstraints = Lists.newArrayList();
        for (String constraintDesc : constraintArray) {
            if (Strings.isNullOrEmpty(constraintDesc)) {
                continue;
            }
            Matcher foreignKeyMatcher = FOREIGN_KEY_PATTERN.matcher(constraintDesc);
            if (!foreignKeyMatcher.find()) {
                LOG.warn("invalid constraint:{}", foreignKeyConstraintDescs);
                continue;
            }
<<<<<<< HEAD
            String sourceColumns = foreignKeyMatcher.group(1);
            String tablePath = foreignKeyMatcher.group(4);
            String targetColumns = foreignKeyMatcher.group(6);
            List<String> baseColumns = Arrays.asList(sourceColumns.split(","))
                    .stream().map(String::trim).collect(Collectors.toList());
            List<String> parentColumns = Arrays.asList(targetColumns.split(","))
                    .stream().map(String::trim).collect(Collectors.toList());

            String[] parts = tablePath.split("\\.");
            Preconditions.checkState(parts.length == 3);
            String catalogName = parts[0];
            String db = parts[1];
            String table = parts[2];

            List<Pair<String, String>> columnRefPairs =
                    Streams.zip(baseColumns.stream(), parentColumns.stream(), Pair::create).collect(Collectors.toList());
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
=======

            String sourceTablePath = foreignKeyMatcher.group(1);
            String sourceColumns = foreignKeyMatcher.group(3);

            String targetTablePath = foreignKeyMatcher.group(6);
            String targetColumns = foreignKeyMatcher.group(8);

            List<String> childColumns = Arrays.stream(sourceColumns.split(",")).
                    map(String::trim).collect(Collectors.toList());
            List<String> parentColumns = Arrays.stream(targetColumns.split(",")).
                    map(String::trim).collect(Collectors.toList());

            String[] targetTableParts = targetTablePath.split("\\.");
            Preconditions.checkState(targetTableParts.length == 3);
            String targetCatalogName = targetTableParts[0];
            String targetDb = targetTableParts[1];
            String targetTable = targetTableParts[2];
            BaseTableInfo parentTableInfo = getTableBaseInfo(targetCatalogName, targetDb, targetTable);

            BaseTableInfo childTableInfo = null;
            if (!Strings.isNullOrEmpty(sourceTablePath)) {
                String[] sourceTableParts = sourceTablePath.split("\\.");
                Preconditions.checkState(sourceTableParts.length == 3);
                String sourceCatalogName = sourceTableParts[0];
                String sourceDb = sourceTableParts[1];
                String sourceTable = sourceTableParts[2];
                childTableInfo = getTableBaseInfo(sourceCatalogName, sourceDb, sourceTable);
            }

            List<Pair<String, String>> columnRefPairs =
                    Streams.zip(childColumns.stream(), parentColumns.stream(), Pair::create).collect(Collectors.toList());
            foreignKeyConstraints.add(new ForeignKeyConstraint(parentTableInfo, childTableInfo, columnRefPairs));
        }
        return foreignKeyConstraints;
    }

    private static BaseTableInfo getTableBaseInfo(String catalog, String db, String table) {
        BaseTableInfo baseTableInfo;
        if (catalog.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            baseTableInfo = new BaseTableInfo(Long.parseLong(db), Long.parseLong(table));
        } else {
            baseTableInfo = new BaseTableInfo(catalog, db, table);
        }
        return baseTableInfo;
    }

    public static String getShowCreateTableConstraintDesc(List<ForeignKeyConstraint> constraints) {
        StringBuilder sb = new StringBuilder();

        List<String> constraintStrs = Lists.newArrayList();
        for (ForeignKeyConstraint constraint : constraints) {
            BaseTableInfo parentTableInfo = constraint.getParentTableInfo();
            BaseTableInfo childTableInfo = constraint.getChildTableInfo();

            StringBuilder constraintSb = new StringBuilder();
            if (childTableInfo != null) {
                constraintSb.append(childTableInfo.getReadableString());
            }
            constraintSb.append("(");
            String baseColumns = Joiner.on(",").join(constraint.getColumnRefPairs()
                    .stream().map(pair -> pair.first).collect(Collectors.toList()));
            constraintSb.append(baseColumns);
            constraintSb.append(")");
            constraintSb.append(" REFERENCES ");
            constraintSb.append(parentTableInfo.getReadableString());

            constraintSb.append("(");
            String parentColumns = Joiner.on(",").join(constraint.getColumnRefPairs()
                    .stream().map(pair -> pair.second).collect(Collectors.toList()));
            constraintSb.append(parentColumns);
            constraintSb.append(")");
            constraintStrs.add(constraintSb.toString());
        }

        sb.append(Joiner.on(";").join(constraintStrs));
        return sb.toString();
    }
>>>>>>> 2.5.18
}
