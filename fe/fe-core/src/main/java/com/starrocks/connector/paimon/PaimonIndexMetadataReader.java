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
import com.starrocks.connector.index.VectorIndexMetric;
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
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
        Map<String, VectorDescriptorCandidate> vectorCandidates = new LinkedHashMap<>();
        Set<String> invalidVectorCandidates = new HashSet<>();
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
            if (indexType == IndexType.VECTOR) {
                Optional<VectorCapability> capability = resolveVectorCapability(
                        provider, globalIndex.indexMeta(), snapshotSchema, field, tableType);
                String candidateKey = field.id() + ":" + provider.toLowerCase(Locale.ROOT);
                if (capability.isEmpty()) {
                    invalidVectorCandidates.add(candidateKey);
                    continue;
                }
                options.put(ConnectorIndexDescriptor.OPTION_METRIC, capability.get().metric.name());
                options.put(ConnectorIndexDescriptor.OPTION_DIMENSION,
                        String.valueOf(capability.get().dimension));
                VectorDescriptorCandidate candidate = new VectorDescriptorCandidate(
                        provider, field, options, capability.get());
                VectorDescriptorCandidate previous = vectorCandidates.putIfAbsent(candidateKey, candidate);
                if (previous != null && !previous.capability.equals(candidate.capability)) {
                    LOG.warn("Ignore inconsistent Paimon {} vector index metadata for field {} at snapshot {}",
                            provider, field.name(), snapshot.id());
                    invalidVectorCandidates.add(candidateKey);
                }
                continue;
            }
            descriptors.add(newIndexDescriptor(indexType, provider, field, options));
        }
        vectorCandidates.forEach((key, candidate) -> {
            if (!invalidVectorCandidates.contains(key)) {
                descriptors.add(newIndexDescriptor(
                        IndexType.VECTOR, candidate.provider, candidate.field, candidate.options));
            }
        });
        return ConnectorIndexMetadata.of(snapshot.id(), tableType, new ArrayList<>(descriptors));
    }

    private static ConnectorIndexDescriptor newIndexDescriptor(
            IndexType indexType, String provider, DataField field, Map<String, String> options) {
        DataType dataType = field.type();
        boolean elementNullable = false;
        if (dataType instanceof org.apache.paimon.types.ArrayType) {
            elementNullable = ((org.apache.paimon.types.ArrayType) dataType).getElementType().isNullable();
        } else if (dataType instanceof VectorType) {
            elementNullable = ((VectorType) dataType).getElementType().isNullable();
        }
        return new ConnectorIndexDescriptor(indexType, provider, field.id(), field.name(),
                dataType.isNullable(), elementNullable, options,
                supportedOperations(indexType, dataType), Coverage.UNKNOWN);
    }

    private static Map<String, String> indexOptions(Map<String, String> tableOptions,
                                                     String columnName, String provider) {
        Map<String, String> result = new HashMap<>();
        String fieldPrefix = "fields." + columnName + ".";
        String normalizedProvider = provider.toLowerCase(Locale.ROOT);
        String providerPrefix = normalizedProvider + ".";
        boolean luminaProvider = isLuminaVectorProvider(normalizedProvider);
        tableOptions.forEach((key, value) -> {
            String normalizedKey = key.toLowerCase(Locale.ROOT);
            if (normalizedKey.startsWith(fieldPrefix.toLowerCase(Locale.ROOT))
                    || normalizedKey.startsWith(providerPrefix)
                    || (luminaProvider && normalizedKey.startsWith("lumina."))) {
                result.put(key, value);
            }
        });
        return result;
    }

    static Optional<VectorIndexMetric> vectorMetric(
            Map<String, String> options, String columnName, String provider, TableType tableType) {
        String normalizedProvider = provider.toLowerCase(Locale.ROOT);
        List<String> keys;
        if (isNativeVectorProvider(normalizedProvider)) {
            // Match NativeVectorGlobalIndexerFactory: field options override index-type options,
            // and the canonical aliases win over their compatibility aliases.
            keys = List.of(
                    "fields." + columnName + ".metric",
                    "fields." + columnName + ".distance.metric",
                    normalizedProvider + ".metric",
                    normalizedProvider + ".distance.metric");
        } else if (isLuminaVectorProvider(normalizedProvider)) {
            keys = List.of(
                    "fields." + columnName + ".distance.metric",
                    "fields." + columnName + ".metric",
                    normalizedProvider + ".distance.metric",
                    normalizedProvider + ".metric",
                    "lumina.distance.metric");
        } else {
            keys = List.of(
                    "fields." + columnName + ".distance.metric",
                    "fields." + columnName + ".metric",
                    normalizedProvider + ".distance.metric",
                    normalizedProvider + ".metric");
        }
        for (String key : keys) {
            Optional<String> value = optionIgnoreCase(options, key);
            if (value.isPresent()) {
                // A malformed more-specific option must not silently fall back to a less-specific one.
                return VectorIndexMetric.fromOption(value.get());
            }
        }
        return Optional.of(VectorIndexMetric.INNER_PRODUCT);
    }

    private static Optional<VectorCapability> resolveVectorCapability(
            String provider, byte[] indexMeta, TableSchema schema, DataField field,
            TableType tableType) {
        String normalizedProvider = provider.toLowerCase(Locale.ROOT);
        Map<String, String> fileMetadata = Collections.emptyMap();
        boolean hasPersistedMetadata = indexMeta != null && indexMeta.length > 0;
        if (hasPersistedMetadata) {
            try {
                fileMetadata = JsonSerdeUtil.parseJsonMap(
                        new String(indexMeta, StandardCharsets.UTF_8), String.class);
            } catch (RuntimeException e) {
                // A malformed persisted payload is not the same as an absent payload. Falling
                // back here could advertise a metric which differs from the bytes on disk.
                LOG.warn("Ignore Paimon {} vector index for field {} because its metadata is invalid",
                        provider, field.name(), e);
                return Optional.empty();
            }
        }

        Map<String, String> fallbackOptions = schema.options();
        Optional<VectorIndexMetric> fallbackMetric;
        if (tableType == TableType.PRIMARY_KEY && isNativeVectorProvider(normalizedProvider)) {
            try {
                CoreOptions coreOptions = new CoreOptions(schema.options());
                if (!coreOptions.primaryKeyVectorIndexColumns().contains(field.name())
                        || !normalizedProvider.equals(
                        coreOptions.primaryKeyVectorIndexType(field.name()).toLowerCase(Locale.ROOT))) {
                    return Optional.empty();
                }
                fallbackMetric = VectorIndexMetric.fromOption(
                        coreOptions.primaryKeyVectorDistanceMetric(field.name()));
                fallbackOptions = coreOptions.primaryKeyVectorIndexOptions(field.name()).toMap();
            } catch (RuntimeException e) {
                LOG.warn("Cannot resolve Paimon primary-key vector options for field {}", field.name(), e);
                return Optional.empty();
            }
        } else {
            fallbackMetric = vectorMetric(schema.options(), field.name(), provider, tableType);
        }

        Optional<String> persistedMetric = firstOptionIgnoreCase(fileMetadata,
                "distance.metric", "metric");
        Optional<String> persistedDimension = firstOptionIgnoreCase(
                fileMetadata, "index.dimension", "dimension");
        if (hasPersistedMetadata
                && (normalizedProvider.equals("lumina") || normalizedProvider.equals("lumina-vector-ann"))
                && (persistedMetric.isEmpty() || persistedDimension.isEmpty())) {
            // LuminaIndexMeta requires both keys. Treat a partial payload as corrupt rather
            // than accidentally pairing on-disk data with today's table defaults.
            return Optional.empty();
        }
        Optional<VectorIndexMetric> metric = persistedMetric.isPresent()
                ? VectorIndexMetric.fromOption(persistedMetric.get())
                : fallbackMetric;
        if (metric.isEmpty()) {
            return Optional.empty();
        }

        Optional<Integer> metadataDimension = persistedDimension.flatMap(PaimonIndexMetadataReader::parsePositiveInteger);
        if (persistedDimension.isPresent() && metadataDimension.isEmpty()) {
            return Optional.empty();
        }
        int dimension;
        if (metadataDimension.isPresent()) {
            dimension = metadataDimension.get();
        } else if (field.type() instanceof VectorType) {
            dimension = ((VectorType) field.type()).getLength();
        } else {
            // Both Paimon native-vector and Lumina use 128 when an ARRAY vector has no
            // explicit dimension. The provider/field precedence mirrors their factories.
            dimension = vectorDimension(fallbackOptions, field.name(), provider).orElse(128);
        }
        return dimension > 0 ? Optional.of(new VectorCapability(metric.get(), dimension)) : Optional.empty();
    }

    static Optional<Integer> vectorDimension(
            Map<String, String> options, String columnName, String provider) {
        String normalizedProvider = provider.toLowerCase(Locale.ROOT);
        List<String> keys;
        if (isNativeVectorProvider(normalizedProvider)) {
            keys = List.of(
                    "fields." + columnName + ".dimension",
                    "fields." + columnName + ".index.dimension",
                    normalizedProvider + ".dimension",
                    normalizedProvider + ".index.dimension");
        } else if (isLuminaVectorProvider(normalizedProvider)) {
            keys = List.of(
                    "fields." + columnName + ".index.dimension",
                    "fields." + columnName + ".dimension",
                    normalizedProvider + ".index.dimension",
                    normalizedProvider + ".dimension",
                    "lumina.index.dimension");
        } else {
            keys = List.of(
                    "fields." + columnName + ".index.dimension",
                    "fields." + columnName + ".dimension",
                    normalizedProvider + ".index.dimension",
                    normalizedProvider + ".dimension");
        }
        for (String key : keys) {
            Optional<String> value = optionIgnoreCase(options, key);
            if (value.isPresent()) {
                return parsePositiveInteger(value.get());
            }
        }
        return Optional.empty();
    }

    private static Optional<String> optionIgnoreCase(Map<String, String> options, String key) {
        return options.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(key))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    private static Optional<String> firstOptionIgnoreCase(Map<String, String> options, String... keys) {
        for (String key : keys) {
            Optional<String> value = optionIgnoreCase(options, key);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    private static Optional<Integer> parsePositiveInteger(String value) {
        if (value == null) {
            return Optional.empty();
        }
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? Optional.of(parsed) : Optional.empty();
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static boolean isNativeVectorProvider(String provider) {
        return provider.equals("ivf-flat") || provider.equals("ivf-pq")
                || provider.equals("ivf-sq") || provider.equals("ivf-rq")
                || provider.equals("diskann");
    }

    private static boolean isLuminaVectorProvider(String provider) {
        return provider.equals("lumina") || provider.equals("lumina-vector-ann");
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
        if ((indexType == IndexType.BITMAP || indexType == IndexType.RANGE)
                && fieldType.isAnyOf(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE)) {
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
            case VECTOR:
                return Set.of(Operation.VECTOR_TOP_N);
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
            case "lumina":
            case "lumina-vector-ann":
            case "ivf-flat":
            case "ivf-pq":
            case "ivf-sq":
            case "ivf-rq":
            case "diskann":
                return IndexType.VECTOR;
            default:
                return null;
        }
    }

    private static final class VectorCapability {
        private final VectorIndexMetric metric;
        private final int dimension;

        private VectorCapability(VectorIndexMetric metric, int dimension) {
            this.metric = metric;
            this.dimension = dimension;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof VectorCapability)) {
                return false;
            }
            VectorCapability that = (VectorCapability) o;
            return dimension == that.dimension && metric == that.metric;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(metric, dimension);
        }
    }

    private static final class VectorDescriptorCandidate {
        private final String provider;
        private final DataField field;
        private final Map<String, String> options;
        private final VectorCapability capability;

        private VectorDescriptorCandidate(String provider, DataField field, Map<String, String> options,
                                          VectorCapability capability) {
            this.provider = provider;
            this.field = field;
            this.options = options;
            this.capability = capability;
        }
    }

}
