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

package com.starrocks.connector.paimon;

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Coverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexerFactory;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;

/** Reads snapshot index manifests and their declared capabilities. */
final class PaimonIndexMetadataReader {
    // Retain the existing log category for metadata diagnostics.
    private static final Logger LOG = LogManager.getLogger(PaimonMetadata.class);

    private PaimonIndexMetadataReader() {
    }

    static ConnectorIndexMetadata load(FileStoreTable fileStoreTable, Snapshot snapshot) {
        Set<ConnectorIndexDescriptor> descriptors = new LinkedHashSet<>();
        TableSchema snapshotSchema = fileStoreTable.schemaManager().schema(snapshot.schemaId());
        if (snapshotSchema == null) {
            throw new StarRocksConnectorException(
                    "Cannot resolve Paimon schema %s for index metadata at snapshot %s",
                    snapshot.schemaId(), snapshot.id());
        }
        TableType tableType = toConnectorIndexTableType(snapshotSchema);
        List<IndexManifestEntry> entries;
        try (Timer ignored = Tracers.watchScope(EXTERNAL, "ScanIndexFileEntries")) {
            entries = fileStoreTable.store().newIndexFileHandler().scan(snapshot, ignoredEntry -> true);
        }
        for (IndexManifestEntry entry : entries) {
            IndexFileMeta indexFile = entry == null ? null : entry.indexFile();
            if (indexFile == null) {
                continue;
            }
            GlobalIndexMeta globalIndex = indexFile.globalIndexMeta();
            String provider = indexFile.indexType();
            IndexType indexType = toConnectorIndexType(provider);
            if (globalIndex == null || indexType == null) {
                continue;
            }
            DataField field = snapshotSchema.idToFieldMap().get(globalIndex.indexFieldId());
            if (field == null) {
                LOG.warn("Ignore Paimon {} index with unknown field id {} at snapshot {}",
                        provider, globalIndex.indexFieldId(), snapshot.id());
                continue;
            }
            if (fileStoreTable.partitionKeys().contains(field.name())) {
                // Partition predicates have already participated in partition pruning. Keeping
                // them out of the global-index condition also prevents an executor from treating
                // partition-only filters as row-id coverage requirements.
                continue;
            }

            Map<String, String> options = indexOptions(snapshotSchema.options(), field.name(), provider);
            descriptors.add(newIndexDescriptor(indexType, provider, field, options));
        }
        return ConnectorIndexMetadata.of(snapshot.id(), tableType, new ArrayList<>(descriptors));
    }

    private static ConnectorIndexDescriptor newIndexDescriptor(
            IndexType indexType, String provider, DataField field, Map<String, String> options) {
        return new ConnectorIndexDescriptor(indexType, provider, field.id(), field.name(),
                options, supportedOperations(indexType, field.type()), Coverage.UNKNOWN);
    }

    private static Map<String, String> indexOptions(Map<String, String> tableOptions,
                                                     String columnName, String provider) {
        Map<String, String> result = new HashMap<>();
        String fieldPrefix = "fields." + columnName + ".";
        String normalizedProvider = provider.toLowerCase(Locale.ROOT);
        String providerPrefix = normalizedProvider + ".";
        tableOptions.forEach((key, value) -> {
            String normalizedKey = key.toLowerCase(Locale.ROOT);
            if (normalizedKey.startsWith(fieldPrefix.toLowerCase(Locale.ROOT))
                    || normalizedKey.startsWith(providerPrefix)) {
                result.put(key, value);
            }
        });
        return result;
    }

    static TableType toConnectorIndexTableType(TableSchema schema) {
        if (!schema.primaryKeys().isEmpty()) {
            return TableType.PRIMARY_KEY;
        }
        return Boolean.parseBoolean(schema.options().getOrDefault(
                CoreOptions.DATA_EVOLUTION_ENABLED.key(), Boolean.FALSE.toString()))
                ? TableType.DATA_EVOLUTION
                : TableType.APPEND_ONLY;
    }

    static Set<Operation> supportedOperations(
            IndexType indexType, DataType fieldType) {
        if (fieldType.isAnyOf(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE)) {
            return Collections.emptySet();
        }
        switch (indexType) {
            case BITMAP:
                return Set.of(Operation.EQUAL, Operation.NOT_EQUAL,
                        Operation.IN, Operation.NOT_IN,
                        Operation.IS_NULL,
                        Operation.IS_NOT_NULL);
            case RANGE:
                return Set.of(Operation.EQUAL, Operation.NOT_EQUAL,
                        Operation.LESS_THAN, Operation.LESS_THAN_OR_EQUAL,
                        Operation.GREATER_THAN, Operation.GREATER_THAN_OR_EQUAL,
                        Operation.IN, Operation.NOT_IN,
                        Operation.IS_NULL,
                        Operation.IS_NOT_NULL, Operation.STARTS_WITH);
            default:
                return Collections.emptySet();
        }
    }

    static IndexType toConnectorIndexType(String indexType) {
        if (indexType == null) {
            return null;
        }
        switch (indexType.toLowerCase(Locale.ROOT)) {
            case BitmapGlobalIndexerFactory.IDENTIFIER:
                return IndexType.BITMAP;
            case BTreeGlobalIndexerFactory.IDENTIFIER:
                return IndexType.RANGE;
            default:
                return null;
        }
    }
}
