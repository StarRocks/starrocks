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

package com.starrocks.connector.share.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializableTable implements Table, Serializable, HasTableOperations {

    private final String name;
    private final String location;
    private final String metadataFileLocation;
    private final Map<String, String> properties;
    private final String schemaAsJson;
    private final int defaultSpecId;
    private final Map<Integer, String> specAsJsonMap;
    private final String sortOrderAsJson;
    private final FileIO io;
    private final EncryptionManager encryption;
    private final LocationProvider locationProvider;
    private final Map<String, SnapshotRef> refs;

    private transient volatile Table lazyTable = null;
    private transient volatile Schema lazySchema = null;
    private transient volatile Map<Integer, PartitionSpec> lazySpecs = null;
    private transient volatile SortOrder lazySortOrder = null;

    public SerializableTable(Table table, FileIO fileIO) {
        this.name = table.name();
        this.location = table.location();
        this.metadataFileLocation = metadataFileLocation(table);
        this.properties = SerializableMap.copyOf(table.properties());
        this.schemaAsJson = SchemaParser.toJson(table.schema());
        this.defaultSpecId = table.spec().specId();
        this.specAsJsonMap = new HashMap<>();
        Map<Integer, PartitionSpec> specs = table.specs();
        specs.forEach((specId, spec) -> specAsJsonMap.put(specId, PartitionSpecParser.toJson(spec)));
        this.sortOrderAsJson = SortOrderParser.toJson(table.sortOrder());
        this.io = fileIO(fileIO);
        this.encryption = table.encryption();
        this.locationProvider = table.locationProvider();
        this.refs = SerializableMap.copyOf(table.refs());
    }

    private String metadataFileLocation(Table table) {
        if (table instanceof HasTableOperations) {
            TableOperations ops = ((HasTableOperations) table).operations();
            return ops.current().metadataFileLocation();
        } else {
            return null;
        }
    }

    private FileIO fileIO(FileIO fileIO) {
        if (fileIO instanceof HadoopConfigurable) {
            ((HadoopConfigurable) fileIO).serializeConfWith(SerializableConfSupplier::new);
        }

        return fileIO;
    }

    private Table lazyTable() {
        if (lazyTable == null) {
            synchronized (this) {
                if (lazyTable == null) {
                    if (metadataFileLocation == null) {
                        throw new UnsupportedOperationException(
                                "Cannot load metadata: metadata file location is null");
                    }

                    TableOperations ops =
                            new StaticTableOperations(metadataFileLocation, io, locationProvider);
                    this.lazyTable = newTable(ops, name);
                }
            }
        }

        return lazyTable;
    }

    protected Table newTable(TableOperations ops, String tableName) {
        return new BaseTable(ops, tableName);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    @Override
    public Schema schema() {
        if (lazySchema == null) {
            synchronized (this) {
                if (lazySchema == null && lazyTable == null) {
                    this.lazySchema = SchemaParser.fromJson(schemaAsJson);
                } else if (lazySchema == null) {
                    this.lazySchema = lazyTable.schema();
                }
            }
        }

        return lazySchema;
    }

    @Override
    public Map<Integer, Schema> schemas() {
        return lazyTable().schemas();
    }

    @Override
    public PartitionSpec spec() {
        return specs().get(defaultSpecId);
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        if (lazySpecs == null) {
            synchronized (this) {
                if (lazySpecs == null && lazyTable == null) {
                    Map<Integer, PartitionSpec> specs = new HashMap<>(specAsJsonMap.size());
                    specAsJsonMap.forEach(
                            (specId, specAsJson) -> {
                                specs.put(specId, PartitionSpecParser.fromJson(schema(), specAsJson));
                            });
                    this.lazySpecs = specs;
                } else if (lazySpecs == null) {
                    this.lazySpecs = lazyTable.specs();
                }
            }
        }

        return lazySpecs;
    }

    @Override
    public SortOrder sortOrder() {
        if (lazySortOrder == null) {
            synchronized (this) {
                if (lazySortOrder == null && lazyTable == null) {
                    this.lazySortOrder = SortOrderParser.fromJson(schema(), sortOrderAsJson);
                } else if (lazySortOrder == null) {
                    this.lazySortOrder = lazyTable.sortOrder();
                }
            }
        }

        return lazySortOrder;
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        return lazyTable().sortOrders();
    }

    @Override
    public FileIO io() {
        return io;
    }

    @Override
    public EncryptionManager encryption() {
        return encryption;
    }

    @Override
    public LocationProvider locationProvider() {
        return locationProvider;
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
        return lazyTable().statisticsFiles();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        return refs;
    }

    @Override
    public void refresh() {
        throw new UnsupportedOperationException(errorMsg("refresh"));
    }

    @Override
    public TableScan newScan() {
        return lazyTable().newScan();
    }

    @Override
    public IncrementalAppendScan newIncrementalAppendScan() {
        return lazyTable().newIncrementalAppendScan();
    }

    @Override
    public IncrementalChangelogScan newIncrementalChangelogScan() {
        return lazyTable().newIncrementalChangelogScan();
    }

    @Override
    public BatchScan newBatchScan() {
        return lazyTable().newBatchScan();
    }

    @Override
    public Snapshot currentSnapshot() {
        return lazyTable().currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return lazyTable().snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return lazyTable().snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
        return lazyTable().history();
    }

    @Override
    public UpdateSchema updateSchema() {
        throw new UnsupportedOperationException(errorMsg("updateSchema"));
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
        throw new UnsupportedOperationException(errorMsg("updateSpec"));
    }

    @Override
    public UpdateProperties updateProperties() {
        throw new UnsupportedOperationException(errorMsg("updateProperties"));
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
        throw new UnsupportedOperationException(errorMsg("replaceSortOrder"));
    }

    @Override
    public UpdateLocation updateLocation() {
        throw new UnsupportedOperationException(errorMsg("updateLocation"));
    }

    @Override
    public AppendFiles newAppend() {
        throw new UnsupportedOperationException(errorMsg("newAppend"));
    }

    @Override
    public RewriteFiles newRewrite() {
        throw new UnsupportedOperationException(errorMsg("newRewrite"));
    }

    @Override
    public RewriteManifests rewriteManifests() {
        throw new UnsupportedOperationException(errorMsg("rewriteManifests"));
    }

    @Override
    public OverwriteFiles newOverwrite() {
        throw new UnsupportedOperationException(errorMsg("newOverwrite"));
    }

    @Override
    public RowDelta newRowDelta() {
        throw new UnsupportedOperationException(errorMsg("newRowDelta"));
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
        throw new UnsupportedOperationException(errorMsg("newReplacePartitions"));
    }

    @Override
    public DeleteFiles newDelete() {
        throw new UnsupportedOperationException(errorMsg("newDelete"));
    }

    @Override
    public UpdateStatistics updateStatistics() {
        throw new UnsupportedOperationException(errorMsg("updateStatistics"));
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
        throw new UnsupportedOperationException(errorMsg("expireSnapshots"));
    }

    @Override
    public ManageSnapshots manageSnapshots() {
        throw new UnsupportedOperationException(errorMsg("manageSnapshots"));
    }

    @Override
    public Transaction newTransaction() {
        throw new UnsupportedOperationException(errorMsg("newTransaction"));
    }

    private String errorMsg(String operation) {
        return String.format("Operation %s is not supported after the table is serialized", operation);
    }

    @Override
    public StaticTableOperations operations() {
        return (StaticTableOperations) ((BaseTable) lazyTable()).operations();
    }

    private static class SerializableConfSupplier implements SerializableSupplier<Configuration> {

        private final Map<String, String> confAsMap;
        private transient volatile Configuration conf = null;

        SerializableConfSupplier(Configuration conf) {
            this.confAsMap = new HashMap<>(conf.size());
            conf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
        }

        @Override
        public Configuration get() {
            if (conf == null) {
                synchronized (this) {
                    if (conf == null) {
                        Configuration newConf = new Configuration(false);
                        confAsMap.forEach(newConf::set);
                        this.conf = newConf;
                    }
                }
            }

            return conf;
        }
    }
}
