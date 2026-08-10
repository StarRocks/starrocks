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

package com.starrocks.connector.starrocks;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TColumnAccessPath;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStarRocksRemoteScanOutput;
import com.starrocks.thrift.TStarRocksRemoteScanRequiredOutput;
import com.starrocks.thrift.TStarRocksRemoteScanWireShape;
import com.starrocks.type.InvalidType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JSON wire contract for the StarRocks catalog control plane (capabilities, list
 * databases/tables, get table, prepare/start remote scan, batch cleanup).
 *
 * <p>The control plane is served over HTTP/JSON rather than the FE thrift port,
 * which is internal-facing and typically not exposed across cluster boundaries.
 * The data plane (BE-to-BE chunk fetch over BRPC / Arrow Flight) is unaffected.
 *
 * <p>The plan-carrying fields ({@link RequiredOutput}, {@link ScanOutput},
 * {@link ColumnAccessPathDto}) mirror the in-memory thrift plan types that the BE
 * still consumes verbatim, but cross the wire as plain JSON: enum values become
 * their {@code name()} string and types become their {@code toSql()} string,
 * round-tripped via {@link StarRocksFeClient#parseType(String)} — the same type
 * SQL round-trip the connector already relies on for column metadata. Converters
 * to/from the thrift types live here so the planner-facing and server-side code
 * keep operating on the thrift plan types unchanged.
 */
public final class StarRocksRemoteScanWire {
    // Wire names of the remote-scan transport, carried in PrepareScanRequest.transport
    // and ScanStream.transport. This is the single source shared by the client-side
    // connector config and the control-plane server.
    public static final String TRANSPORT_BRPC_CHUNK = "brpc_chunk";
    public static final String TRANSPORT_ARROW_FLIGHT = "arrow_flight";

    private StarRocksRemoteScanWire() {
    }

    // ---- type SQL <-> thrift TTypeDesc helpers -----------------------------

    private static String typeToSql(Type type) {
        if (type == null || type == InvalidType.INVALID) {
            return null;
        }
        return type.toSql();
    }

    private static Type sqlToType(String typeSql) {
        if (Strings.isNullOrEmpty(typeSql)) {
            return InvalidType.INVALID;
        }
        return StarRocksFeClient.parseType(typeSql);
    }

    private static String typeDescToSql(com.starrocks.thrift.TTypeDesc typeDesc) {
        if (typeDesc == null) {
            return null;
        }
        return TypeDeserializer.fromThrift(typeDesc).toSql();
    }

    private static com.starrocks.thrift.TTypeDesc sqlToTypeDesc(String typeSql) {
        if (Strings.isNullOrEmpty(typeSql)) {
            return null;
        }
        return TypeSerializer.toThrift(StarRocksFeClient.parseType(typeSql));
    }

    // ---- ColumnAccessPath <-> DTO ------------------------------------------

    public static final class ColumnAccessPathDto {
        @SerializedName("type")
        public String type;
        @SerializedName("path")
        public String path;
        @SerializedName("value_type")
        public String valueType;
        @SerializedName("from_predicate")
        public boolean fromPredicate;
        @SerializedName("extended")
        public boolean extended;
        @SerializedName("children")
        public List<ColumnAccessPathDto> children;
    }

    public static ColumnAccessPathDto toDto(ColumnAccessPath path) {
        if (path == null) {
            return null;
        }
        ColumnAccessPathDto dto = new ColumnAccessPathDto();
        dto.type = path.getType() == null ? null : path.getType().name();
        dto.path = path.getPath();
        dto.valueType = typeToSql(path.getValueType());
        dto.fromPredicate = path.isFromPredicate();
        dto.extended = path.isExtended();
        if (path.getChildren() != null && !path.getChildren().isEmpty()) {
            dto.children = new ArrayList<>(path.getChildren().size());
            for (ColumnAccessPath child : path.getChildren()) {
                dto.children.add(toDto(child));
            }
        }
        return dto;
    }

    public static ColumnAccessPath toDomain(ColumnAccessPathDto dto) {
        if (dto == null || Strings.isNullOrEmpty(dto.type)) {
            throw new IllegalArgumentException("column access path misses type");
        }
        ColumnAccessPath path = new ColumnAccessPath(TAccessPathType.valueOf(dto.type), dto.path, sqlToType(dto.valueType));
        path.setFromPredicate(dto.fromPredicate);
        path.setExtended(dto.extended);
        if (dto.children != null) {
            for (ColumnAccessPathDto child : dto.children) {
                path.addChildPath(toDomain(child));
            }
        }
        return path;
    }

    private static ColumnAccessPathDto toDto(TColumnAccessPath thrift) {
        return thrift == null ? null : toDto(ColumnAccessPath.fromThrift(thrift));
    }

    private static TColumnAccessPath toThriftAccessPath(ColumnAccessPathDto dto) {
        return dto == null ? null : toDomain(dto).toThrift();
    }

    // ---- required output (request side) <-> DTO ----------------------------

    public static final class RequiredOutput {
        @SerializedName("local_slot_id")
        public int localSlotId;
        @SerializedName("root_column")
        public String rootColumn;
        @SerializedName("access_path")
        public ColumnAccessPathDto accessPath;
        @SerializedName("wire_shape")
        public String wireShape;
        @SerializedName("expected_wire_type")
        public String expectedWireType;
    }

    public static RequiredOutput toDto(TStarRocksRemoteScanRequiredOutput thrift) {
        if (thrift == null) {
            return null;
        }
        RequiredOutput dto = new RequiredOutput();
        dto.localSlotId = thrift.isSetLocal_slot_id() ? thrift.local_slot_id : -1;
        dto.rootColumn = thrift.root_column;
        dto.accessPath = thrift.isSetAccess_path() ? toDto(thrift.access_path) : null;
        dto.wireShape = thrift.wire_shape == null ? null : thrift.wire_shape.name();
        dto.expectedWireType = thrift.isSetExpected_wire_type() ? typeDescToSql(thrift.expected_wire_type) : null;
        return dto;
    }

    public static TStarRocksRemoteScanRequiredOutput toThrift(RequiredOutput dto) {
        if (dto == null) {
            return null;
        }
        TStarRocksRemoteScanRequiredOutput thrift = new TStarRocksRemoteScanRequiredOutput();
        thrift.setLocal_slot_id(dto.localSlotId);
        if (!Strings.isNullOrEmpty(dto.rootColumn)) {
            thrift.setRoot_column(dto.rootColumn);
        }
        if (dto.accessPath != null) {
            thrift.setAccess_path(toThriftAccessPath(dto.accessPath));
        }
        if (!Strings.isNullOrEmpty(dto.wireShape)) {
            thrift.setWire_shape(TStarRocksRemoteScanWireShape.valueOf(dto.wireShape));
        }
        com.starrocks.thrift.TTypeDesc expectedWireType = sqlToTypeDesc(dto.expectedWireType);
        if (expectedWireType != null) {
            thrift.setExpected_wire_type(expectedWireType);
        }
        return thrift;
    }

    // ---- scan output (response side) <-> DTO -------------------------------

    public static final class ScanOutput {
        @SerializedName("output_index")
        public int outputIndex;
        @SerializedName("local_slot_id")
        public int localSlotId;
        @SerializedName("remote_slot_id")
        public int remoteSlotId;
        @SerializedName("name")
        public String name;
        @SerializedName("actual_wire_type")
        public String actualWireType;
        @SerializedName("nullable")
        public boolean nullable;
        @SerializedName("is_const")
        public boolean isConst;
        @SerializedName("wire_shape")
        public String wireShape;
    }

    public static ScanOutput toDto(TStarRocksRemoteScanOutput thrift) {
        if (thrift == null) {
            return null;
        }
        ScanOutput dto = new ScanOutput();
        dto.outputIndex = thrift.isSetOutput_index() ? thrift.output_index : -1;
        dto.localSlotId = thrift.isSetLocal_slot_id() ? thrift.local_slot_id : -1;
        dto.remoteSlotId = thrift.isSetRemote_slot_id() ? thrift.remote_slot_id : -1;
        dto.name = thrift.name;
        dto.actualWireType = thrift.isSetActual_wire_type() ? typeDescToSql(thrift.actual_wire_type) : null;
        dto.nullable = thrift.isSetNullable() && thrift.nullable;
        dto.isConst = thrift.isSetIs_const() && thrift.is_const;
        dto.wireShape = thrift.wire_shape == null ? null : thrift.wire_shape.name();
        return dto;
    }

    public static TStarRocksRemoteScanOutput toThrift(ScanOutput dto) {
        if (dto == null) {
            return null;
        }
        TStarRocksRemoteScanOutput thrift = new TStarRocksRemoteScanOutput();
        thrift.setOutput_index(dto.outputIndex);
        thrift.setLocal_slot_id(dto.localSlotId);
        if (dto.remoteSlotId >= 0) {
            thrift.setRemote_slot_id(dto.remoteSlotId);
        }
        if (!Strings.isNullOrEmpty(dto.name)) {
            thrift.setName(dto.name);
        }
        com.starrocks.thrift.TTypeDesc actualWireType = sqlToTypeDesc(dto.actualWireType);
        if (actualWireType != null) {
            thrift.setActual_wire_type(actualWireType);
        }
        thrift.setNullable(dto.nullable);
        thrift.setIs_const(dto.isConst);
        if (!Strings.isNullOrEmpty(dto.wireShape)) {
            thrift.setWire_shape(TStarRocksRemoteScanWireShape.valueOf(dto.wireShape));
        }
        return thrift;
    }

    // ---- host:port <-> DTO -------------------------------------------------

    public static final class HostPort {
        @SerializedName("host")
        public String host;
        @SerializedName("port")
        public int port;
    }

    public static HostPort toDto(TNetworkAddress address) {
        if (address == null) {
            return null;
        }
        HostPort dto = new HostPort();
        dto.host = address.hostname;
        dto.port = address.port;
        return dto;
    }

    public static TNetworkAddress toThrift(HostPort dto) {
        if (dto == null) {
            return null;
        }
        return new TNetworkAddress(dto.host, dto.port);
    }

    // ---- column metadata ---------------------------------------------------

    public static final class Column {
        @SerializedName("name")
        public String name;
        @SerializedName("type")
        public String type;
        @SerializedName("nullable")
        public boolean nullable;
        @SerializedName("is_partition_column")
        public boolean isPartitionColumn;
    }

    // ---- scan stream (prepare response) ------------------------------------

    public static final class ScanStream {
        @SerializedName("scan_token")
        public String scanToken;
        @SerializedName("remote_be")
        public HostPort remoteBe;
        @SerializedName("transport")
        public String transport;
    }

    // ---- response envelope base --------------------------------------------

    public static class StatusEnvelope {
        @SerializedName("status")
        public int status;
        @SerializedName("exception")
        public String exception;
    }

    // ---- capabilities ------------------------------------------------------

    public static final class CapabilitiesResponse extends StatusEnvelope {
        @SerializedName("cluster_id")
        public int clusterId;
        @SerializedName("version")
        public String version;
        @SerializedName("supported_transports")
        public List<String> supportedTransports;
        @SerializedName("feature_flags")
        public List<String> featureFlags;
        // Only the Arrow Flight port is cluster-wide; each serving BE's brpc endpoint travels
        // per stream in the prepare_scan response.
        @SerializedName("arrow_flight_port")
        public int arrowFlightPort;
    }

    // ---- list databases / tables -------------------------------------------

    public static final class ListDatabasesResponse extends StatusEnvelope {
        @SerializedName("databases")
        public List<String> databases;
    }

    public static final class ListTablesResponse extends StatusEnvelope {
        @SerializedName("tables")
        public List<String> tables;
    }

    // ---- get table ---------------------------------------------------------

    public static final class Table {
        @SerializedName("db")
        public String db;
        @SerializedName("table")
        public String table;
        @SerializedName("schema_version")
        public long schemaVersion;
        @SerializedName("columns")
        public List<Column> columns;
        @SerializedName("partition_columns")
        public List<String> partitionColumns;
        @SerializedName("row_count")
        public long rowCount;
        // Remote table id: unique within the remote cluster and never reused, which is what
        // StarRocksExternalTable.getUUID() needs as an incarnation marker.
        @SerializedName("table_id")
        public long tableId;
    }

    public static final class GetTableResponse extends StatusEnvelope {
        @SerializedName("table")
        public Table table;
    }

    // ---- prepare remote scan -----------------------------------------------

    public static final class PrepareScanRequest {
        @SerializedName("db")
        public String db;
        @SerializedName("table")
        public String table;
        @SerializedName("schema_version")
        public long schemaVersion;
        @SerializedName("required_columns")
        public List<String> requiredColumns;
        @SerializedName("required_outputs")
        public List<RequiredOutput> requiredOutputs;
        @SerializedName("column_access_paths")
        public List<ColumnAccessPathDto> columnAccessPaths;
        @SerializedName("pushdown_predicate_sql")
        public String pushdownPredicateSql;
        @SerializedName("soft_limit")
        public long softLimit;
        @SerializedName("session_id")
        public String sessionId;
        @SerializedName("transport")
        public String transport;
        @SerializedName("session_vars")
        public Map<String, String> sessionVars;
    }

    public static final class PrepareScanResponse extends StatusEnvelope {
        @SerializedName("session_id")
        public String sessionId;
        @SerializedName("scan_token")
        public String scanToken;
        @SerializedName("remote_bes")
        public List<HostPort> remoteBes;
        @SerializedName("streams")
        public List<ScanStream> streams;
        @SerializedName("output_schema")
        public List<Column> outputSchema;
        @SerializedName("outputs")
        public List<ScanOutput> outputs;
        @SerializedName("properties")
        public Map<String, String> properties;
    }

    // ---- start scan / batch cleanup ----------------------------------------

    public static final class ScanControlRequest {
        @SerializedName("session_id")
        public String sessionId;
    }

    public static final class CleanupItem {
        @SerializedName("session_id")
        public String sessionId;
        @SerializedName("cancel")
        public boolean cancel;

        public CleanupItem() {
        }

        public CleanupItem(String sessionId, boolean cancel) {
            this.sessionId = sessionId;
            this.cancel = cancel;
        }
    }

    public static final class BatchCleanupRequest {
        @SerializedName("items")
        public List<CleanupItem> items;
    }

    public static final class SimpleResponse extends StatusEnvelope {
    }
}
