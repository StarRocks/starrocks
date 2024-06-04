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

package com.starrocks.sql.automv.tunespace;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.util.PrettyPrinter;
import com.starrocks.sql.automv.util.TieredMap;
import com.starrocks.sql.optimizer.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class MaterializedViewPlus {
    private static final ImmutableSet<String> COMPLEX_PROPERTY = ImmutableSet.<String>builder()
            .add(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
            .add(PropertyAnalyzer.PROPERTIES_BF_FPP)
            .add(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)
            .add(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)
            .add(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)
            .add(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)
            .add(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)
            .add(PropertyAnalyzer.PROPERTY_MV_SORT_KEYS)
            .build();
    private final MaterializedView mv;
    private final TableName fqName;
    private transient List<String> columns = null;
    private transient List<String> columnComments = null;
    private transient List<String> indices = null;
    private transient List<List<String>> indexColumns = null;
    private transient Optional<String> partition = null;
    private transient List<String> partitionKey = null;
    private transient List<String> distributionKey = null;
    private transient Integer bucketNum = null;
    private transient String distribution = null;
    private transient String refreshScheme = null;

    private MaterializedViewPlus(MaterializedView mv, TableName fqName) {
        this.mv = Objects.requireNonNull(mv);
        this.fqName = Objects.requireNonNull(fqName);
    }

    public static MaterializedViewPlus of(MaterializedView mv, TableName fqName) {
        return new MaterializedViewPlus(mv, fqName);
    }

    private static FQTable baseTableInfoToFqTable(BaseTableInfo baseTableInfo) {
        Catalog catalog =
                GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(baseTableInfo.getCatalogName());
        TableName tableName =
                new TableName(baseTableInfo.getCatalogName(), baseTableInfo.getDbName(), baseTableInfo.getTableName());
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Optional<Database> database = metadataMgr.getDatabase(baseTableInfo);
        Optional<Table> table = metadataMgr.getTable(baseTableInfo);
        if (database.isEmpty()) {
            throw new RuntimeException(
                    String.format("Database %s.%s is absent", baseTableInfo.getCatalogName(),
                            baseTableInfo.getDbName()));

        }
        if (table.isEmpty()) {
            throw new RuntimeException(
                    String.format("Table %s.%s.%s is absent", baseTableInfo.getCatalogName(), baseTableInfo.getDbName(),
                            baseTableInfo.getTableName()));
        }
        return FQTable.of(catalog, database.get(), table.get(), tableName);
    }

    public MaterializedView getMv() {
        return mv;
    }

    public TableName getFqName() {
        return fqName;
    }

    public List<String> getColumns() {
        if (columns == null) {
            List<Integer> outputIndices =
                    CollectionUtils.isNotEmpty(mv.getQueryOutputIndices()) ? mv.getQueryOutputIndices() :
                            IntStream.range(0, mv.getBaseSchema().size()).boxed().collect(Collectors.toList());
            List<Column> columnList = outputIndices.stream().map(mv.getBaseSchema()::get).collect(Collectors.toList());
            columns = columnList.stream().map(Column::getName).collect(ImmutableList.toImmutableList());
            columnComments = columnList.stream().map(Column::getComment).collect(ImmutableList.toImmutableList());
        }
        return columns;
    }

    public List<String> getColumnComments() {
        if (columnComments == null) {
            getColumns();
        }
        return Objects.requireNonNull(columnComments);
    }

    public List<String> getIndices() {
        if (indices == null) {
            List<Index> indexList = Optional.ofNullable(mv.getIndexes()).orElseGet(Collections::emptyList);
            indices = indexList.stream().map(Index::toSql).collect(ImmutableList.toImmutableList());
            indexColumns = indexList.stream().map(Index::getColumns).collect(ImmutableList.toImmutableList());
        }
        return indices;
    }

    public List<List<String>> getIndexColumns() {
        if (indexColumns == null) {
            getIndices();
        }
        return Objects.requireNonNull(indexColumns);
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(mv.getComment());
    }

    public Optional<String> getPartition() {
        if (partition == null) {
            partition = Optional.ofNullable(mv.getPartitionInfo())
                    .map(pi -> pi.isPartitioned() ? pi.toSql(mv, Collections.emptyList()) : null);
        }
        return partition;
    }

    public List<String> getPartitionKey() {
        if (partitionKey == null) {
            partitionKey = Optional.ofNullable(mv.getPartitionColumnNames()).orElseGet(Collections::emptyList);
        }
        return partitionKey;
    }

    public String getDistribution() {
        if (distribution == null) {
            distribution = mv.getDefaultDistributionInfo().toSql();
        }
        return distribution;
    }

    public List<String> getDistributionKey() {
        if (distributionKey == null) {
            DistributionInfo distributionInfo = mv.getDefaultDistributionInfo();
            if (distributionInfo instanceof HashDistributionInfo) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                distributionKey = hashDistributionInfo.getDistributionColumns().stream()
                        .map(Column::getName).collect(ImmutableList.toImmutableList());
                bucketNum = hashDistributionInfo.getBucketNum();
            } else if (distributionInfo instanceof RandomDistributionInfo) {
                RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) distributionInfo;
                distributionKey = Collections.emptyList();
                bucketNum = randomDistributionInfo.getBucketNum();
            } else {
                distributionKey = Collections.emptyList();
                bucketNum = 0;
            }
        }
        return distributionKey;
    }

    public int getBucketNum() {
        if (bucketNum == null) {
            getDistributionKey();
        }
        return Objects.requireNonNull(bucketNum);
    }

    public List<String> getSortKey() {
        return Optional.ofNullable(mv.getTableProperty().getMvSortKeys()).orElseGet(Collections::emptyList);
    }

    private String getRefreshSchemeImpl() {
        // refresh scheme
        PrettyPrinter printer = new PrettyPrinter();
        MaterializedView.MvRefreshScheme refreshScheme = mv.getRefreshScheme();
        if (refreshScheme == null) {
            printer.add("REFRESH UNKNOWN");
        } else {
            if (refreshScheme.getMoment().equals(MaterializedView.RefreshMoment.DEFERRED)) {
                printer.add("REFRESH ").add(refreshScheme.getMoment()).spaces(1).add(refreshScheme.getType());
            } else {
                printer.add("REFRESH ").add(refreshScheme.getType());
            }
        }

        if (refreshScheme != null && refreshScheme.getType() == MaterializedView.RefreshType.ASYNC) {
            MaterializedView.AsyncRefreshContext asyncRefreshContext = refreshScheme.getAsyncRefreshContext();
            if (asyncRefreshContext.isDefineStartTime()) {
                printer.add(" START(")
                        .addEscapedDoubleQuoted(Utils.getDatetimeFromLong(asyncRefreshContext.getStartTime())
                                .format(DateUtils.DATE_TIME_FORMATTER))
                        .add(")");
            }
            if (asyncRefreshContext.getTimeUnit() != null) {
                printer.add(" EVERY(INTERVAL ").add(asyncRefreshContext.getStep()).spaces(1)
                        .add(asyncRefreshContext.getTimeUnit()).add(")");
            }
        }
        return printer.getResult();
    }

    public String getRefreshScheme() {
        if (refreshScheme == null) {
            refreshScheme = getRefreshSchemeImpl();
        }
        return Objects.requireNonNull(refreshScheme);
    }

    public TieredMap<String, String> getSimpleProperties() {
        Map<String, String> properties = mv.getTableProperty().getProperties();
        TieredMap.Builder<String, String> propBuilder = TieredMap.newGenesisTier();
        properties.entrySet().stream()
                .filter(e -> !COMPLEX_PROPERTY.contains(e.getKey()))
                .forEach(propBuilder::put);

        Optional.ofNullable(properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME))
                .ifPresent(t -> propBuilder.put(
                        PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                        TimeUtils.longToTimeString(Long.parseLong(t))));

        if (mv instanceof LakeMaterializedView) {
            String volume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                    .getStorageVolumeNameOfTable(mv.getId());
            propBuilder.put(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME, volume);
        } else {
            propBuilder.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, mv.getStorageMedium());
        }
        return propBuilder.build();
    }

    private TieredMap<String, String> getSomeProperties(String... names) {
        Preconditions.checkArgument(names.length > 0);
        Map<String, String> properties = mv.getTableProperty().getProperties();
        TieredMap.Builder<String, String> propBuilder = TieredMap.newGenesisTier();
        Arrays.asList(names).forEach(name -> Optional.ofNullable(properties.get(name))
                .ifPresent(value -> propBuilder.put(name, value)));
        return propBuilder.build();
    }

    public TieredMap<String, String> getBfProperties(Set<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return TieredMap.genesis();
        }
        TieredMap.Builder<String, String> bfProps = TieredMap.newGenesisTier();
        bfProps.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, String.join(", ", columns));
        Optional.ofNullable(mv.getProperties().get(PropertyAnalyzer.PROPERTIES_BF_FPP))
                .ifPresent(value -> {
                    bfProps.put(PropertyAnalyzer.PROPERTIES_BF_FPP, value);
                });

        return bfProps.build();
    }

    public Set<String> getBfColumns() {
        return Optional.ofNullable(mv.getBfColumns()).orElseGet(Collections::emptySet);
    }

    public TieredMap<String, String> getColocateProperties() {
        return getSomeProperties(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
    }

    public List<Pair<List<String>, ForeignKeyConstraint>> getForeignKeyConstraints() {
        List<ForeignKeyConstraint> fkConstraints =
                Optional.ofNullable(mv.getForeignKeyConstraints()).orElseGet(Collections::emptyList);
        List<Pair<List<String>, ForeignKeyConstraint>> columnsToFkConstraints =
                Lists.newArrayListWithCapacity(fkConstraints.size());
        fkConstraints.forEach(fk -> {
            FQTable parentFqTable = baseTableInfoToFqTable(fk.getParentTableInfo());
            FQTable childFqTable = baseTableInfoToFqTable(fk.getChildTableInfo());
            String parentFqName = parentFqTable.getFQName();
            String childFqName = childFqTable.getFQName();
            List<String> columns = fk.getColumnRefPairs().stream()
                    .flatMap(p -> Stream.of(parentFqName + "." + p.first, childFqName + "." + p.second))
                    .collect(Collectors.toList());
            columnsToFkConstraints.add(Pair.create(columns, fk));
        });

        return columnsToFkConstraints;
    }

    public List<Pair<List<String>, UniqueConstraint>> getUniqueConstraints() {
        List<UniqueConstraint> ukConstraints =
                Optional.ofNullable(mv.getUniqueConstraints()).orElseGet(Collections::emptyList);

        List<Pair<List<String>, UniqueConstraint>> columnToUkConstraints =
                Lists.newArrayListWithCapacity(ukConstraints.size());
        ukConstraints.forEach(uk -> {
            TableName fqTableName = new TableName(uk.getCatalogName(), uk.getDbName(), uk.getTableName());
            String fqName = fqTableName.toString();
            List<String> columns =
                    uk.getUniqueColumns().stream().map(name -> fqName + "." + name).collect(Collectors.toList());
            columnToUkConstraints.add(Pair.create(columns, uk));
        });
        return columnToUkConstraints;
    }

    public List<List<String>> getForeignKeyColumns() {
        return getForeignKeyConstraints().stream().map(fk -> fk.first).collect(Collectors.toList());
    }

    public List<String> getForeignKeys() {
        return getForeignKeyConstraints().stream()
                .map(fk -> ForeignKeyConstraint.getShowCreateTableConstraintDesc(Collections.singletonList(fk.second)))
                .collect(Collectors.toList());
    }

    public TieredMap<String, String> getForeignKeyProperties(java.util.function.Predicate<List<String>> predicate) {
        Preconditions.checkArgument(columns != null);
        List<ForeignKeyConstraint> fkConstraints =
                getForeignKeyConstraints().stream()
                        .filter(p -> predicate.test(p.first))
                        .map(p -> p.second).collect(Collectors.toList());
        if (fkConstraints.isEmpty()) {
            return TieredMap.genesis();
        } else {
            String value = ForeignKeyConstraint.getShowCreateTableConstraintDesc(fkConstraints);
            return TieredMap.<String, String>newGenesisTier()
                    .put(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT, value)
                    .build();
        }
    }

    public List<List<String>> getUniqueKeyColumns() {
        return getUniqueConstraints().stream().map(p -> p.first).collect(Collectors.toList());
    }

    public List<String> getUniqueKeys() {
        return getUniqueConstraints().stream().map(p -> p.second.toString()).collect(Collectors.toList());
    }

    public TieredMap<String, String> getUniqueKeyProperties(java.util.function.Predicate<List<String>> predicate) {
        List<UniqueConstraint> ukConstraints = getUniqueConstraints().stream()
                .filter(p -> predicate.test(p.first))
                .map(p -> p.second)
                .collect(Collectors.toList());
        if (ukConstraints.isEmpty()) {
            return TieredMap.genesis();
        } else {
            String value = ukConstraints.stream()
                    .map(UniqueConstraint::toString).collect(Collectors.joining("; "));
            return TieredMap.<String, String>newGenesisTier()
                    .put(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT, value)
                    .build();
        }
    }

    public String getCreateMaterializedViewSql() {
        PrettyPrinter printer = new PrettyPrinter();
        printer.add("CREATE MATERIALIZED VIEW ").add(fqName.toSql()).add(" (").newLine();

        List<PrettyPrinter> columnPrinters = IntStream.range(0, getColumns().size()).mapToObj(i -> {
            PrettyPrinter columnPrinter = new PrettyPrinter();
            columnPrinter.addBacktickQuoted(getColumns().get(i));
            Optional.ofNullable(getColumnComments().get(i)).ifPresent(comment -> {
                if (!comment.isEmpty()) {
                    columnPrinter.add(" COMMENT ").addEscapedDoubleQuoted(comment);
                }
            });
            return columnPrinter;
        }).collect(Collectors.toList());

        printer.indentEnclose(() -> {
            printer.addSuperStepsWithDelNl(",", columnPrinters);
        });
        if (!getIndices().isEmpty()) {
            printer.indentEnclose(() -> {
                printer.newLine().add(",");
                printer.addItemsWithNlDel(",", getIndices());
            });
        }
        printer.newLine().add(")").newLine();
        getComment().ifPresent(comment -> printer.add("COMMENT ").addEscapedDoubleQuoted(comment).newLine());
        getPartition().ifPresent(partition -> printer.add(partition).newLine());
        printer.add(getDistribution()).newLine();
        if (!getSortKey().isEmpty()) {
            printer.add("ORDER BY (").addItemsBacktickQuoted(", ", getSortKey()).add(")").newLine();
        }
        printer.add(getRefreshScheme()).newLine();
        printer.add("PROPERTIES (").newLine();
        List<PrettyPrinter> properties = getSimpleProperties()
                .merge(getBfProperties(getBfColumns()))
                .merge(getColocateProperties())
                .merge(getUniqueKeyProperties(s -> true))
                .merge(getForeignKeyProperties(s -> true))
                .entrySet().stream()
                .map(e -> new PrettyPrinter().addDoubleQuoted(e.getKey()).add(" = ").addDoubleQuoted(e.getValue()))
                .collect(Collectors.toList());
        printer.indentEnclose(() -> {
            printer.addSuperStepsWithDelNl(",", properties);
        });
        printer.newLine().add(")").newLine();
        printer.add("AS").newLine().add(mv.getViewDefineSql());
        return printer.getResult();
    }
}
