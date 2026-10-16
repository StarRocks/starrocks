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

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.SessionVariable.PaimonReaderMode;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackDataSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Predicate;

/** Plan-time compatibility checks for the packaged paimon-cpp 0.3.0 reader. */
public final class PaimonReaderSelector {
    public enum Reader { STARROCKS, NATIVE, JNI }

    public enum Reason {
        RAW_FILES, NATIVE_SUPPORTED, EXPLICIT_MODE, SYSTEM_SPLIT, INDEXED_SPLIT,
        UNKNOWN_SCHEMA, FILE_FORMAT, TABLE_SCHEMA, SCHEMA_EVOLUTION, MERGE_FUNCTION,
        DELETION_VECTOR, ORC_LEGACY_TIMESTAMP, TIMESTAMP, NULLABLE_MAP_KEY, SPLIT_PROTOCOL
    }

    public record Decision(Reader reader, Reason reason) { }

    private static final Set<String> CPP_FORMATS = Set.of("parquet", "orc", "avro", "blob");
    private static final Set<String> VALUE_AGGREGATES = Set.of(
            "first_value", "last_value", "first_non_null_value", "last_non_null_value");

    private final FileStoreTable table;
    private final TableSchema schema;
    private final Set<String> fields;
    private final List<DataType> selectedTypes;
    private final LongFunction<TableSchema> schemaLoader;
    // A selector belongs to one scan node. Schema ids are immutable within its table/branch.
    private final Map<Long, TableSchema> schemas = new HashMap<>();
    private final Map<Long, SchemaFields> schemaFields = new HashMap<>();
    private PaimonReaderSelector fallback;

    private record SchemaFields(Map<String, DataField> names, Map<Integer, DataField> ids) {
        SchemaFields(TableSchema schema) {
            this(schema.nameToFieldMap(), schema.idToFieldMap());
        }
    }

    public PaimonReaderSelector(Table table, List<String> fields) {
        this.table = table instanceof FileStoreTable ? (FileStoreTable) table : null;
        this.schema = this.table == null ? null : this.table.schema();
        this.fields = new HashSet<>(fields);
        this.selectedTypes = selectedTypes(schema != null ? schema.fields() :
                table == null ? List.of() : table.rowType().getFields(), this.fields);
        this.schemaLoader = id -> this.table.schemaManager().schema(id);
        if (schema != null) {
            schemas.put(schema.id(), schema);
        }
    }

    PaimonReaderSelector(TableSchema schema, List<String> fields, LongFunction<TableSchema> schemaLoader) {
        this.table = null;
        this.schema = schema;
        this.fields = new HashSet<>(fields);
        this.selectedTypes = selectedTypes(schema.fields(), this.fields);
        this.schemaLoader = schemaLoader;
        schemas.put(schema.id(), schema);
    }

    public Decision select(Split split, PaimonReaderMode mode) {
        DataSplit data = PaimonSplitUtils.getDataSplit(split).orElse(null);
        if (data == null) {
            return jni(Reason.SYSTEM_SPLIT);
        }
        boolean indexed = PaimonSplitUtils.isGlobalIndexSplit(split);
        if (indexed && data instanceof FallbackDataSplit) {
            return requireJni(PaimonReaderMode.NATIVE, Reason.SPLIT_PROTOCOL);
        }
        if (data instanceof FallbackDataSplit && ((FallbackDataSplit) data).isFallback()) {
            if (mode == PaimonReaderMode.JNI) {
                return jni(Reason.EXPLICIT_MODE);
            }
            if (table == null) {
                return requireJni(mode, Reason.UNKNOWN_SCHEMA);
            }
            if (fallback == null) {
                String branch = schema.options().get("scan.fallback-branch");
                if (branch == null) {
                    return requireJni(mode, Reason.UNKNOWN_SCHEMA);
                }
                fallback = new PaimonReaderSelector(table.switchToBranch(branch), List.copyOf(fields));
            }
            // CPP reads the fallback branch's schema, but the SR output tuple uses the main schema.
            if (!schema.logicalRowType().equals(fallback.schema.logicalRowType())) {
                return requireJni(mode, Reason.SCHEMA_EVOLUTION);
            }
            // FallbackDataSplit.wrapped() returns itself. Resolve metadata once, without re-entering this branch.
            return fallback.selectData(data, mode, false);
        }
        return selectData(data, mode, indexed);
    }

    private Decision selectData(DataSplit data, PaimonReaderMode mode, boolean indexed) {
        if (!indexed && mode == PaimonReaderMode.JNI) {
            return jni(Reason.EXPLICIT_MODE);
        }
        if (schema == null) {
            return requireJni(indexed ? PaimonReaderMode.NATIVE : mode, Reason.UNKNOWN_SCHEMA);
        }
        if (!indexed && mode == PaimonReaderMode.AUTO && rawSupported(data)) {
            return new Decision(Reader.STARROCKS, Reason.RAW_FILES);
        }
        Reason unsupported = nativeUnsupported(data, mode == PaimonReaderMode.AUTO);
        if (unsupported != null) {
            return requireJni(indexed ? PaimonReaderMode.NATIVE : mode, unsupported);
        }
        return new Decision(Reader.NATIVE, indexed ? Reason.INDEXED_SPLIT :
                mode == PaimonReaderMode.NATIVE ? Reason.EXPLICIT_MODE : Reason.NATIVE_SUPPORTED);
    }

    private Decision requireJni(PaimonReaderMode mode, Reason reason) {
        if (mode == PaimonReaderMode.NATIVE) {
            throw new StarRocksConnectorException(
                    "Paimon native reader cannot read this split: %s. Select a compatible reader or exclude " +
                            "unsupported columns.", reason);
        }
        return jni(reason);
    }

    private Decision jni(Reason reason) {
        for (DataTypeRoot unsupported : List.of(DataTypeRoot.BLOB, DataTypeRoot.VARIANT)) {
            if (selectedTypes.stream().anyMatch(type -> contains(type, t -> t.is(unsupported)))) {
                String message = unsupported == DataTypeRoot.VARIANT ?
                        "Reading VARIANT through the Paimon JNI reader is not yet supported" :
                        "Unsupported type: BLOB in Paimon JNI reader";
                throw new StarRocksConnectorException(
                        "%s. Native reader is required for this column (%s).", message, reason);
            }
        }
        return new Decision(Reader.JNI, reason);
    }

    private boolean rawSupported(DataSplit split) {
        var rawFiles = split.convertToRawFiles();
        if (rawFiles.isEmpty() || selectedTypes.stream()
                .anyMatch(type -> contains(type, t -> t.is(DataTypeRoot.BLOB)))) {
            return false;
        }
        return rawFiles.get().stream().allMatch(raw -> {
            if (!raw.format().equals("parquet") && !raw.format().equals("orc")) {
                return false;
            }
            if (selectedTypes.stream().anyMatch(type -> contains(type, t -> timestampPrecision(t) > 6 ||
                    (raw.format().equals("orc") && isTemporal(t))))) {
                return false;
            }
            TableSchema fileSchema = fileSchema(raw.schemaId());
            return fields.stream().allMatch(name -> sameField(fileSchema, name));
        });
    }

    private Reason nativeUnsupported(DataSplit split, boolean conservative) {
        if (!CPP_FORMATS.contains(schema.options().getOrDefault("file.format", "parquet")) ||
                split.dataFiles().stream().anyMatch(file -> !CPP_FORMATS.contains(file.fileFormat()))) {
            return Reason.FILE_FORMAT;
        }
        if (!nativeSchemaSupported(schema)) {
            // CPP parses the complete table schema before applying column projection.
            return Reason.TABLE_SCHEMA;
        }
        Set<String> required = new HashSet<>(fields);
        // Legacy files without delete counts still go through CPP's merge reader, even if raw-convertible.
        boolean merge = !schema.primaryKeys().isEmpty() && !split.isStreaming() && split.bucket() != -2 &&
                (!split.rawConvertible() || split.dataFiles().stream().anyMatch(file -> file.deleteRowCount().isEmpty()));
        if (merge) {
            required.addAll(schema.primaryKeys());
            addNames(required, schema.options().get("sequence.field"));
            int previousSize;
            do {
                previousSize = required.size();
                schema.options().forEach((key, value) -> {
                    if (key.startsWith("fields.") && key.endsWith(".sequence-group") &&
                            Arrays.stream(value.split(",")).map(String::trim).anyMatch(required::contains)) {
                        required.add(key.substring("fields.".length(), key.length() - ".sequence-group".length()));
                    }
                });
            } while (required.size() != previousSize);
            if (!mergeSupported(required)) {
                return Reason.MERGE_FUNCTION;
            }
        }
        for (var file : split.dataFiles()) {
            TableSchema old = fileSchema(file.schemaId());
            if (!nativeSchemaSupported(old)) {
                return Reason.TABLE_SCHEMA;
            }
            if (!CPP_FORMATS.contains(old.options().getOrDefault("file.format", "parquet"))) {
                return Reason.FILE_FORMAT;
            }
            if (split.deletionFiles().isPresent() && split.deletionFiles().get().stream().anyMatch(f -> f != null) &&
                    (option(schema, "deletion-vectors.bitmap64", false) ||
                            option(old, "deletion-vectors.bitmap64", false))) {
                return Reason.DELETION_VECTOR;
            }
            // The ORC adapter may initialize unprojected fields from the file schema as well.
            if (file.fileFormat().equals("orc") && (option(schema, "orc.timestamp-ltz.legacy.type", true) ||
                    option(old, "orc.timestamp-ltz.legacy.type", true)) &&
                    old.fields().stream().anyMatch(f -> contains(f.type(),
                            t -> t.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)))) {
                return Reason.ORC_LEGACY_TIMESTAMP;
            }
            for (String name : required) {
                DataField current = fieldMaps(schema).names().get(name);
                if (current == null) {
                    continue;
                }
                DataField previous = fieldMaps(old).ids().get(current.id());
                // Explicit NATIVE retains CPP's conversion semantics; AUTO only uses proven conversions.
                if (conservative && previous != null && !safeNativeEvolution(previous.type(), current.type())) {
                    return Reason.SCHEMA_EVOLUTION;
                }
                if (conservative && contains(current.type(), PaimonReaderSelector::isTimestamp)) {
                    // Negative fractional timestamps and nanosecond overflow cannot be ruled out by type alone.
                    return Reason.TIMESTAMP;
                }
                if (conservative && !file.fileFormat().equals("parquet") && contains(current.type(),
                        t -> t instanceof MapType && ((MapType) t).getKeyType().isNullable())) {
                    return Reason.NULLABLE_MAP_KEY;
                }
            }
        }
        return null;
    }

    private boolean mergeSupported(Set<String> required) {
        String engine = schema.options().getOrDefault("merge-engine", "deduplicate");
        if (engine.equals("deduplicate") || engine.equals("first-row")) {
            return true;
        }
        if (!engine.equals("aggregation") && !engine.equals("partial-update")) {
            return false;
        }
        Set<String> sequences = new HashSet<>();
        addNames(sequences, schema.options().get("sequence.field"));
        for (String name : required) {
            if (schema.primaryKeys().contains(name) || sequences.contains(name)) {
                continue;
            }
            DataField field = fieldMaps(schema).names().get(name);
            String function = schema.options().getOrDefault("fields." + name + ".aggregate-function",
                    schema.options().getOrDefault("fields.default-aggregate-function", "last_non_null_value"));
            if (field == null || VALUE_AGGREGATES.contains(function)) {
                continue;
            }
            DataTypeRoot type = field.type().getTypeRoot();
            boolean numeric = integralRank(type) > 0 || type == DataTypeRoot.FLOAT ||
                    type == DataTypeRoot.DOUBLE || type == DataTypeRoot.DECIMAL;
            if ((function.equals("sum") && numeric) ||
                    ((function.equals("bool_or") || function.equals("bool_and")) && type == DataTypeRoot.BOOLEAN) ||
                    (function.equals("listagg") && field.type().isAnyOf(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR)) ||
                    ((function.equals("min") || function.equals("max")) && (numeric ||
                            field.type().isAnyOf(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR, DataTypeRoot.BINARY,
                                    DataTypeRoot.VARBINARY, DataTypeRoot.DATE) || isTimestamp(field.type())))) {
                continue;
            }
            return false;
        }
        return true;
    }

    private TableSchema fileSchema(long id) {
        return schemas.computeIfAbsent(id, schemaLoader::apply);
    }

    private SchemaFields fieldMaps(TableSchema schema) {
        return schemaFields.computeIfAbsent(schema.id(), id -> new SchemaFields(schema));
    }

    private static List<DataType> selectedTypes(List<DataField> schemaFields, Set<String> fields) {
        return schemaFields.stream().filter(field -> fields.contains(field.name()))
                .map(DataField::type).toList();
    }

    private boolean sameField(TableSchema old, String name) {
        DataField current = fieldMaps(schema).names().get(name);
        if (current == null) {
            return false;
        }
        DataField previous = fieldMaps(old).ids().get(current.id());
        if (previous == null) {
            return !fieldMaps(old).names().containsKey(name);
        }
        return previous.name().equals(name) && previous.type().equalsIgnoreNullable(current.type());
    }

    private static boolean safeNativeEvolution(DataType old, DataType current) {
        if (old.equalsIgnoreNullable(current)) {
            return true;
        }
        int from = integralRank(old.getTypeRoot());
        int to = integralRank(current.getTypeRoot());
        return (from > 0 && to >= from) || (old.is(DataTypeRoot.FLOAT) && current.is(DataTypeRoot.DOUBLE));
    }

    private static boolean nativeSchemaSupported(TableSchema schema) {
        return schema.fields().stream().noneMatch(field -> contains(field.type(), t -> t.isAnyOf(
                DataTypeRoot.TIME_WITHOUT_TIME_ZONE, DataTypeRoot.MULTISET, DataTypeRoot.VECTOR)));
    }

    private static int integralRank(DataTypeRoot type) {
        return switch (type) {
            case TINYINT -> 1;
            case SMALLINT -> 2;
            case INTEGER -> 3;
            case BIGINT -> 4;
            default -> 0;
        };
    }

    private static boolean option(TableSchema schema, String key, boolean fallback) {
        return Boolean.parseBoolean(schema.options().getOrDefault(key, Boolean.toString(fallback)));
    }

    private static void addNames(Set<String> names, String value) {
        if (value != null) {
            Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(names::add);
        }
    }

    private static boolean isTimestamp(DataType type) {
        return type.isAnyOf(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    private static boolean isTemporal(DataType type) {
        return isTimestamp(type) || type.is(DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
    }

    private static int timestampPrecision(DataType type) {
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        }
        return type instanceof LocalZonedTimestampType ? ((LocalZonedTimestampType) type).getPrecision() : 0;
    }

    private static boolean contains(DataType type, Predicate<DataType> predicate) {
        if (predicate.test(type)) {
            return true;
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream().anyMatch(field -> contains(field.type(), predicate));
        }
        if (type instanceof ArrayType) {
            return contains(((ArrayType) type).getElementType(), predicate);
        }
        if (type instanceof MapType) {
            return contains(((MapType) type).getKeyType(), predicate) ||
                    contains(((MapType) type).getValueType(), predicate);
        }
        return false;
    }
}
