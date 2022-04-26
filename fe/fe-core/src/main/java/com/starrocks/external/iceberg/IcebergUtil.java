// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.external.hive.HdfsFileFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergUtil {
    /**
     * Get Iceberg table identifier by table property
     */
    public static TableIdentifier getIcebergTableIdentifier(IcebergTable table) {
        return TableIdentifier.of(table.getDb(), table.getTable());
    }

    /**
     * Get Iceberg table identifier by table property
     */
    public static TableIdentifier getIcebergTableIdentifier(String db, String table) {
        return TableIdentifier.of(db, table);
    }

    /**
     * Returns the corresponding globalStateMgr implementation.
     */
    public static IcebergCatalog getIcebergCatalog(IcebergTable table)
            throws StarRocksIcebergException {
        IcebergCatalogType catalogType = table.getCatalogType();
        switch (catalogType) {
            case HIVE_CATALOG:
                return getIcebergHiveCatalog(table.getIcebergHiveMetastoreUris());
            case CUSTOM_CATALOG:
                return getIcebergCustomCatalog(table.getCatalogImpl(), table.getIcebergProperties());
            default:
                throw new StarRocksIcebergException(
                        "Unexpected globalStateMgr type: " + catalogType.toString());
        }
    }

    /**
     * Returns the corresponding hive globalStateMgr implementation.
     */
    public static IcebergCatalog getIcebergHiveCatalog(String metastoreUris)
            throws StarRocksIcebergException {
        return IcebergHiveCatalog.getInstance(metastoreUris);
    }

    /**
     * Returns the corresponding custom globalStateMgr implementation.
     */
    public static IcebergCatalog getIcebergCustomCatalog(String catalogImpl, Map<String, String> icebergProperties)
            throws StarRocksIcebergException {
        return (IcebergCatalog) CatalogLoader.custom(String.format("Custom-%s", catalogImpl),
                new Configuration(), icebergProperties, catalogImpl).loadCatalog();
    }

    /**
     * Get hdfs file format in StarRocks use iceberg file format.
     *
     * @param format
     * @return HdfsFileFormat
     */
    public static HdfsFileFormat getHdfsFileFormat(FileFormat format) {
        switch (format) {
            case ORC:
                return HdfsFileFormat.ORC;
            case PARQUET:
                return HdfsFileFormat.PARQUET;
            default:
                throw new StarRocksIcebergException(
                        "Unexpected file format: " + format.toString());
        }
    }

    /**
     * Get current snapshot of iceberg table, return null if snapshot do not exist.
     * Refresh table is needed.
     *
     * @param table
     * @return Optional<Snapshot>
     */
    public static Optional<Snapshot> getCurrentTableSnapshot(Table table, boolean refresh) {
        if (refresh) {
            refreshTable(table);
        }
        return Optional.ofNullable(table.currentSnapshot());
    }

    /**
     * Get table scan for given table and snapshot, filter with given iceberg predicates.
     * Refresh table if needed.
     *
     * @param table
     * @param snapshot
     * @param icebergPredicates
     * @param refresh
     * @return
     */
    public static TableScan getTableScan(Table table,
                                         Snapshot snapshot,
                                         List<Expression> icebergPredicates,
                                         boolean refresh) {
        if (refresh) {
            refreshTable(table);
        }

        TableScan tableScan = table.newScan().useSnapshot(snapshot.snapshotId()).includeColumnStats();
        Expression filterExpressions = Expressions.alwaysTrue();
        if (!icebergPredicates.isEmpty()) {
            filterExpressions = icebergPredicates.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
        }

        return tableScan.filter(filterExpressions);
    }

    private static void refreshTable(Table table) {
        try {
            if (table instanceof BaseTable) {
                BaseTable baseTable = (BaseTable) table;
                if (baseTable.operations().refresh() == null) {
                    // If table is loaded successfully, current table metadata will never be null.
                    // So when we get a null metadata after refresh, it indicates the table has been dropped.
                    // See: https://github.com/StarRocks/starrocks/issues/3076
                    throw new NoSuchTableException("No such table: %s", table.name());
                }
            } else {
                // table loaded by GlobalStateMgr should be a base table
                throw new StarRocksIcebergException(String.format("Invalid table type of %s, it should be a BaseTable!",
                        table.name()));
            }
        } catch (NoSuchTableException e) {
            throw new StarRocksIcebergException(String.format("No such table  %s", table.name()));
        } catch (IllegalStateException ei) {
            throw new StarRocksIcebergException(String.format("Refresh table %s with failure, the table under hood" +
                            " may have been dropped. You should re-create the external table. cause %s",
                    table.name(), ei.getMessage()));
        }
    }

    /**
     * @param partitionSpec
     * @return
     */
    public static Map<PartitionField, Integer> getIdentityPartitions(PartitionSpec partitionSpec) {
        // TODO: expose transform information in Iceberg library
        ImmutableMap.Builder<PartitionField, Integer> columns = ImmutableMap.builder();
        for (int i = 0; i < partitionSpec.fields().size(); i++) {
            PartitionField field = partitionSpec.fields().get(i);
            if (field.transform().toString().equals("identity")) {
                columns.put(field, i);
            }
        }
        return columns.build();
    }
}
