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


package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Infer key for each operator from the query plan
 */
public class KeyInference extends OptExpressionVisitor<KeyInference.KeyPropertySet, Void> {

    private static final KeyInference INSTANCE = new KeyInference();

    private KeyInference() {
    }

    public static KeyPropertySet infer(OptExpression optExpr, Void ctx) {
        KeyPropertySet keySet = optExpr.getOp().accept(INSTANCE, optExpr, ctx);
        return keySet;
    }

    @Override
    public KeyPropertySet visit(OptExpression optExpression, Void ctx) {
        throw new NotImplementedException("Operator not supported:" + optExpression);
    }

    @Override
    public KeyPropertySet visitPhysicalFilter(OptExpression optExpression, Void ctx) {
        return infer(optExpression.inputAt(0), ctx);
    }

    @Override
    public KeyPropertySet visitPhysicalProject(OptExpression optExpression, Void ctx) {
        KeyPropertySet input = infer(optExpression.inputAt(0), ctx);
        PhysicalProjectOperator project = (PhysicalProjectOperator) optExpression.getOp();
        ColumnRefSet projectColumns = new ColumnRefSet(project.getOutputColumns());

        KeyPropertySet res = new KeyPropertySet();
        for (KeyProperty key : input.getKeys()) {
            if (key.unique && key.columns.containsAll(projectColumns)) {
                res.addKey(key);
            }
        }
        res.addKey(KeyProperty.of(projectColumns, false));

        return res;
    }

    private KeyPropertySet visitTable(OlapTable olapTable, PhysicalOperator scan,
                                      List<ColumnRefOperator> outputColumns,
                                      Map<ColumnRefOperator, Column> columnMap) {
        // If the table contains unique key and output the whole unique-keys
        List<Column> tableKeyColumns = olapTable.getKeyColumns();
        Set<Column> keyRefs = outputColumns.stream().map(columnMap::get).collect(Collectors.toSet());
        boolean unique = olapTable.getKeysType().equals(KeysType.PRIMARY_KEYS) &&
                keyRefs.containsAll(tableKeyColumns);

        KeyProperty key = KeyProperty.of(new ColumnRefSet(outputColumns), unique);
        KeyPropertySet res = new KeyPropertySet();
        res.addKey(key);
        return res;
    }

    @Override
    public KeyPropertySet visitPhysicalOlapScan(OptExpression optExpression, Void ctx) {
        PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();
        Table table = scan.getTable();
        Preconditions.checkState(table.isOlapTable());
        OlapTable olapTable = (OlapTable) table;

        return visitTable(olapTable, scan, scan.getRealOutputColumns(), scan.getColRefToColumnMetaMap());
    }

    public static class KeyProperty {
        public final boolean unique;
        public final ColumnRefSet columns;

        public KeyProperty(ColumnRefSet columns, boolean unique) {
            this.columns = columns;
            this.unique = unique;
        }

        public static KeyProperty of(ColumnRefSet columns) {
            return new KeyProperty(columns, false);
        }

        public static KeyProperty of(ColumnRefSet columns, boolean unique) {
            return new KeyProperty(columns, unique);
        }

        public static KeyProperty ofUnique(ColumnRefSet columns) {
            return new KeyProperty(columns, true);
        }

        public static KeyProperty combine(KeyProperty lhs, KeyProperty rhs) {
            ColumnRefSet columns = new ColumnRefSet();
            columns.union(lhs.columns);
            columns.union(rhs.columns);
            return new KeyProperty(columns, lhs.unique && rhs.unique);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyProperty that = (KeyProperty) o;
            return unique == that.unique && Objects.equals(columns, that.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(unique, columns);
        }

        public String format(Map<Integer, String> colMap) {
            String colNames = columns.getStream().map(colMap::get).collect(Collectors.joining(","));
            return "Key{" +
                    "unique=" + unique +
                    ", columns=" + colNames +
                    "}";
        }

        @Override
        public String toString() {
            return "Key{" +
                    "unique=" + unique +
                    ", columns=" + columns +
                    '}';
        }
    }

    public static class KeyPropertySet {
        private List<KeyProperty> keySet = new ArrayList<>();

        public KeyPropertySet() {
        }

        public KeyPropertySet(KeyProperty key) {
            this.keySet = Collections.singletonList(key);
        }

        public KeyPropertySet(List<KeyProperty> keys) {
            this.keySet = keys;
        }

        public boolean empty() {
            return CollectionUtils.isEmpty(keySet);
        }

        public List<KeyProperty> getKeys() {
            return keySet;
        }

        public KeyProperty getBestKey() {
            sortKeys();
            return keySet.get(0);
        }

        /**
         * Sort keys, prefer unique key and shorter keys
         */
        public void sortKeys() {
            this.keySet.sort((x, y) -> {
                if (x.unique && !y.unique) {
                    return -1;
                }
                if (!x.unique && y.unique) {
                    return 1;
                }
                return x.columns.size() - y.columns.size();
            });
        }

        public void addKeys(List<KeyProperty> keys) {
            this.keySet.addAll(keys);
        }

        public void addKey(KeyProperty key) {
            this.keySet.add(key);
        }
    }

}
