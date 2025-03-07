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

package com.starrocks.sql.util;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TablePlus {

    private final OlapTable table;

    private final Class<?> klass;
    private final int replicationNum;
    private final List<ColumnPlus> columnPluses;

    private TablePlus(OlapTable table, Class<?> klass, List<ColumnPlus> columnPluses, int replicationNum) {
        this.table = Objects.requireNonNull(table);
        this.klass = Objects.requireNonNull(klass);
        this.columnPluses = Objects.requireNonNull(columnPluses);
        this.replicationNum = replicationNum;
    }

    public static TablePlus of(OlapTable table, Class<?> klass, List<ColumnPlus> columns, int replicationNum) {
        return new TablePlus(table, klass, columns, replicationNum);
    }

    public OlapTable getTable() {
        return table;
    }

    public List<ColumnPlus> getColumnPluses() {
        return columnPluses;
    }

    public int getReplicationNum() {
        return replicationNum;
    }

    public String getCreateTableSql() {
        PrettyPrinter printer = new PrettyPrinter();
        printer.add("CREATE TABLE IF NOT EXISTS").spaces(1).add(table.getName()).add("(").newLine();
        List<Column> columns = table.getFullSchema();
        List<String> columnDefinitions = columns.stream()
                .map(col -> col.toSqlWithoutAggregateTypeName(table.getIdToColumn())).collect(Collectors.toList());
        printer.indentEnclose(() -> {
            printer.addItemsWithDelNl(",", columnDefinitions);
        });
        printer.newLine().add(")").spaces(1).add("ENGINE=OLAP").newLine();

        List<String> key = columns.stream().filter(Column::isKey).map(Column::getName).collect(Collectors.toList());
        printer.add(table.getKeysType().toSql()).add("(").addItems(", ", key).add(")").newLine();

        int numBuckets = table.getDefaultDistributionInfo().getBucketNum();
        String bucketKey = table.getDefaultDistributionInfo().getDistributionKey(table.getIdToColumn());
        printer.add("DISTRIBUTED BY HASH(").add(bucketKey).add(")").spaces(1)
                .add("BUCKETS").spaces(1).add(numBuckets).newLine();

        printer.add("PROPERTIES").spaces(1).add("(").newLine();
        printer.indentEnclose(() -> {
            printer.addDoubleQuoted("replication_num").add(" = ").addDoubleQuoted(replicationNum);
        });
        printer.newLine().add(")");
        return printer.getResult();
    }

    public <T> String getInsertSql(Collection<T> objects) {
        Preconditions.checkArgument(!objects.isEmpty());
        Preconditions.checkArgument(objects.stream().map(Object::getClass).allMatch(klass::isAssignableFrom));

        PrettyPrinter printer = new PrettyPrinter();
        List<String> columns = table.getFullSchema().stream()
                .filter(column -> !column.isAutoIncrement())
                .map(Column::getName).collect(Collectors.toList());
        printer.add("INSERT INTO ").add(table.getName()).add("(")
                .addItems(", ", columns).add(")").add(" VALUES ").newLine();
        Function<Object, PrettyPrinter> tupleMaker = ColumnPlus.toTuple(columnPluses);
        List<PrettyPrinter> tuples = objects.stream().map(tupleMaker).collect(Collectors.toList());
        printer.indentEnclose(() -> {
            printer.addSuperStepsWithDelNl(",", tuples);
        });
        return printer.getResult();
    }

    public String getInsertAsSelectSql(String fqName) {
        PrettyPrinter printer = new PrettyPrinter();
        List<String> columns = table.getFullSchema().stream()
                .filter(column -> !column.isAutoIncrement())
                .map(Column::getName).collect(Collectors.toList());

        printer.add("INSERT INTO ").add(table.getName()).add("(")
                .addItems(", ", columns).add(")").newLine()
                .add("SELECT ").addItems(", ", columns).add(" FROM ").add(fqName);
        return printer.getResult();
    }

    public String getSelectSql(List<String> items, Supplier<List<String>> conjunctsBuilder) {
        Preconditions.checkArgument(!items.isEmpty());
        PrettyPrinter printer = new PrettyPrinter();
        printer.add("SELECT ").newLine();
        printer.indentEnclose(() -> printer.addItemsWithNlDel(",", items));
        printer.newLine().add("FROM ").add(table.getName());
        List<String> conjuncts = conjunctsBuilder.get();
        if (ObjectUtils.isNotEmpty(conjuncts)) {
            printer.newLine().add("WHERE").newLine();
            printer.indentEnclose(() -> printer.addItemsWithNlDel("AND ", conjuncts));
        }
        return printer.getResult();
    }
}
