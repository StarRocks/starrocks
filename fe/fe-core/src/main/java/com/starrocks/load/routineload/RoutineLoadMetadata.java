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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.DdlException;
import com.starrocks.load.Load;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportMetadataStmt;
import com.starrocks.thrift.TStreamSourceMetaKind;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.Set;

// Routine-load source metadata (INCLUDE METADATA clause).
// INCLUDE METADATA (<KEY> AS <alias>, ...) binds per-message Kafka/Pulsar metadata to hidden source
// columns the BE scanner fills from the message rather than the payload, so a payload field named
// like a metadata field is never shadowed. Each alias becomes a source column referenced by COLUMNS
// exprs. The kinds mirror TStreamSourceMetaKind in PlanNodes.thrift.
public class RoutineLoadMetadata {
    private RoutineLoadMetadata() {
    }

    public enum StreamMetaKind {
        TOPIC, PARTITION, OFFSET, MESSAGE_ID, TIMESTAMP, EVENT_TIME, KEY, HEADERS
    }

    // The metadata kind and fixed column type a metadata key maps to.
    public static final class StreamMetaKeySpec {
        public final StreamMetaKind kind;
        public final Type type;

        public StreamMetaKeySpec(StreamMetaKind kind, Type type) {
            this.kind = kind;
            this.type = type;
        }
    }

    // Source -> (metadata key -> spec). Keys are user-facing and source-dependent; several keys may map
    // to the same kind (Kafka TIMESTAMP_MS and Pulsar PUBLISH_TIME_MS both -> TIMESTAMP; HEADERS and
    // PROPERTIES both -> HEADERS). Matched case-insensitively by metaKeySpecOf.
    private static final Map<String, Map<String, StreamMetaKeySpec>> META_KEYS = buildMetaKeys();

    private static Map<String, Map<String, StreamMetaKeySpec>> buildMetaKeys() {
        Type vc = VarcharType.VARCHAR;
        Type bigint = IntegerType.BIGINT;
        Type intType = IntegerType.INT;
        Type map = new MapType(VarcharType.VARCHAR, VarcharType.VARCHAR);
        Map<String, StreamMetaKeySpec> kafka = Maps.newHashMap();
        kafka.put("TOPIC", new StreamMetaKeySpec(StreamMetaKind.TOPIC, vc));
        kafka.put("KEY", new StreamMetaKeySpec(StreamMetaKind.KEY, vc));
        kafka.put("PARTITION", new StreamMetaKeySpec(StreamMetaKind.PARTITION, intType));
        kafka.put("OFFSET", new StreamMetaKeySpec(StreamMetaKind.OFFSET, bigint));
        kafka.put("TIMESTAMP_MS", new StreamMetaKeySpec(StreamMetaKind.TIMESTAMP, bigint));
        kafka.put("HEADERS", new StreamMetaKeySpec(StreamMetaKind.HEADERS, map));
        Map<String, StreamMetaKeySpec> pulsar = Maps.newHashMap();
        pulsar.put("TOPIC", new StreamMetaKeySpec(StreamMetaKind.TOPIC, vc));
        pulsar.put("KEY", new StreamMetaKeySpec(StreamMetaKind.KEY, vc));
        pulsar.put("PARTITION", new StreamMetaKeySpec(StreamMetaKind.PARTITION, intType));
        pulsar.put("MESSAGE_ID", new StreamMetaKeySpec(StreamMetaKind.MESSAGE_ID, vc));
        pulsar.put("PUBLISH_TIME_MS", new StreamMetaKeySpec(StreamMetaKind.TIMESTAMP, bigint));
        pulsar.put("EVENT_TIME_MS", new StreamMetaKeySpec(StreamMetaKind.EVENT_TIME, bigint));
        pulsar.put("PROPERTIES", new StreamMetaKeySpec(StreamMetaKind.HEADERS, map));
        Map<String, Map<String, StreamMetaKeySpec>> m = Maps.newHashMap();
        m.put("KAFKA", kafka);
        m.put("PULSAR", pulsar);
        return m;
    }

    // The spec for metadata `key` under `dataSourceType`, or null if the key is unsupported for that
    // source. Case-insensitive on both source and key.
    public static StreamMetaKeySpec metaKeySpecOf(String dataSourceType, String key) {
        if (dataSourceType == null || key == null) {
            return null;
        }
        Map<String, StreamMetaKeySpec> bySource = META_KEYS.get(dataSourceType.toUpperCase());
        if (bySource == null) {
            return null;
        }
        return bySource.get(key.toUpperCase());
    }

    // Validates the INCLUDE METADATA clause against the job's data source and format. Throws on a
    // non-JSON/Avro format (CSV maps one message to many rows, so per-message metadata is ambiguous),
    // Avro on a Pulsar job (PulsarTaskInfo sends non-JSON data to the BE as CSV), an unsupported key for
    // the source, an empty/duplicate alias, or an alias colliding with a payload (bare COLUMNS) field or
    // a synthetic column (__op, __starrocks*). The destination-table collision is checked in initColumns
    // where the table is available. Called at CREATE and ALTER.
    public static void validateIncludeMetadata(ImportMetadataStmt metadata, List<ImportColumnDesc> columnDescs,
                                               String dataSourceType, String format) throws DdlException {
        if (metadata == null || metadata.getItems() == null || metadata.getItems().isEmpty()) {
            return;
        }
        boolean isJson = "json".equalsIgnoreCase(format);
        boolean isAvro = "avro".equalsIgnoreCase(format);
        if (!isJson && !isAvro) {
            throw new DdlException("INCLUDE METADATA requires the data format to be JSON or Avro");
        }
        if (isAvro && "PULSAR".equalsIgnoreCase(dataSourceType)) {
            throw new DdlException("INCLUDE METADATA requires 'format' = 'json' for Pulsar routine load; "
                    + "Pulsar does not support Avro");
        }
        Set<String> payloadFields = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        if (columnDescs != null) {
            for (ImportColumnDesc desc : columnDescs) {
                if (desc.isColumn()) {
                    payloadFields.add(desc.getColumnName());
                }
            }
        }
        Set<String> seen = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (ImportMetadataStmt.Item item : metadata.getItems()) {
            if (metaKeySpecOf(dataSourceType, item.getKey()) == null) {
                Map<String, StreamMetaKeySpec> supported =
                        META_KEYS.get(dataSourceType == null ? "" : dataSourceType.toUpperCase());
                String hint = supported == null ? ""
                        : " Supported keys for " + dataSourceType + ": "
                                + String.join(", ", Sets.newTreeSet(supported.keySet())) + ".";
                throw new DdlException(item.getKey() + " metadata is not supported for " + dataSourceType
                        + " routine load." + hint);
            }
            String alias = item.getAlias();
            if (alias == null || alias.isEmpty()) {
                throw new DdlException("INCLUDE METADATA requires an alias (AS <name>) for key " + item.getKey());
            }
            if (!seen.add(alias)) {
                throw new DdlException("duplicate INCLUDE METADATA alias: " + alias);
            }
            if (payloadFields.contains(alias)) {
                throw new DdlException("INCLUDE METADATA alias '" + alias + "' collides with a COLUMNS field");
            }
            if (alias.equalsIgnoreCase(Load.LOAD_OP_COLUMN) || alias.toLowerCase().startsWith("__starrocks")) {
                throw new DdlException("INCLUDE METADATA alias '" + alias + "' uses a reserved column name");
            }
        }
    }

    // The set of metadata kinds the INCLUDE METADATA clause references; drives the BE consumer gates.
    public static Set<StreamMetaKind> collectStreamMetaKinds(ImportMetadataStmt metadata, String dataSourceType) {
        Set<StreamMetaKind> kinds = Sets.newHashSet();
        if (metadata == null || metadata.getItems() == null) {
            return kinds;
        }
        for (ImportMetadataStmt.Item item : metadata.getItems()) {
            StreamMetaKeySpec spec = metaKeySpecOf(dataSourceType, item.getKey());
            if (spec != null) {
                kinds.add(spec.kind);
            }
        }
        return kinds;
    }

    // A hidden source column bound to a message-metadata kind. The alias (hiddenName) is the user-visible
    // column name COLUMNS exprs reference; the scanner fills the slot from the message by slot id.
    public static final class StreamMetaBinding {
        public final String hiddenName;
        public final StreamMetaKind kind;
        public final Type type;

        StreamMetaBinding(String hiddenName, StreamMetaKind kind, Type type) {
            this.hiddenName = hiddenName;
            this.kind = kind;
            this.type = type;
        }
    }

    // Deep copy of the column mappings so planning can rewrite expr trees in place without touching the
    // job's persisted ImportColumnDesc/Expr objects (which SHOW CREATE ROUTINE LOAD renders).
    public static List<ImportColumnDesc> deepCopyColumnDescs(List<ImportColumnDesc> columnExprs) {
        List<ImportColumnDesc> copy = Lists.newArrayList();
        for (ImportColumnDesc desc : columnExprs) {
            if (desc.getExpr() != null) {
                copy.add(new ImportColumnDesc(desc.getColumnName(), desc.getExpr().clone(),
                        desc.isMaterialized(), desc.getPos()));
            } else {
                copy.add(new ImportColumnDesc(desc.getColumnName(), desc.isMaterialized()));
            }
        }
        return copy;
    }

    // Builds the metadata bindings from the INCLUDE METADATA clause for `dataSourceType`. The alias is
    // the hidden source-column name; the kind/type come from META_KEYS. Skips keys unsupported by the
    // source (already rejected by validateIncludeMetadata). Returns empty for no clause.
    public static List<StreamMetaBinding> bindingsFromMetadata(ImportMetadataStmt metadata, String dataSourceType) {
        List<StreamMetaBinding> bindings = Lists.newArrayList();
        if (metadata == null || metadata.getItems() == null) {
            return bindings;
        }
        for (ImportMetadataStmt.Item item : metadata.getItems()) {
            StreamMetaKeySpec spec = metaKeySpecOf(dataSourceType, item.getKey());
            if (spec == null) {
                continue;
            }
            bindings.add(new StreamMetaBinding(item.getAlias(), spec.kind, spec.type));
        }
        return bindings;
    }

    public static TStreamSourceMetaKind toTStreamSourceMetaKind(StreamMetaKind kind) {
        switch (kind) {
            case TOPIC:
                return TStreamSourceMetaKind.TOPIC;
            case PARTITION:
                return TStreamSourceMetaKind.PARTITION;
            case OFFSET:
                return TStreamSourceMetaKind.OFFSET;
            case MESSAGE_ID:
                return TStreamSourceMetaKind.MESSAGE_ID;
            case TIMESTAMP:
                return TStreamSourceMetaKind.TIMESTAMP;
            case EVENT_TIME:
                return TStreamSourceMetaKind.EVENT_TIME;
            case KEY:
                return TStreamSourceMetaKind.KEY;
            case HEADERS:
                return TStreamSourceMetaKind.HEADERS;
            default:
                throw new IllegalStateException("unknown stream meta kind: " + kind);
        }
    }
}
