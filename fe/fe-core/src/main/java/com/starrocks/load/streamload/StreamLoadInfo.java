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

package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.google.re2j.Pattern;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.ImportWhereStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RowDelimiter;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_NAME;

public class StreamLoadInfo {

    private static final Logger LOG = LogManager.getLogger(StreamLoadInfo.class);
    private static final Pattern PART_NAME_SPLIT = Pattern.compile("\\s*,\\s*");

    private TUniqueId id;
    private long txnId;
    private TFileType fileType;
    private TFileFormatType formatType;
    private boolean stripOuterArray;
    private String jsonPaths;
    private String jsonRoot;

    // optional
    private List<ImportColumnDesc> columnExprDescs = Lists.newArrayList();
    private Expr whereExpr;
    private String mergeConditionStr;
    private ColumnSeparator columnSeparator;
    private RowDelimiter rowDelimiter;
    private long skipHeader;
    private boolean trimSpace;
    private byte enclose;
    private byte escape;
    private PartitionNames partitions;
    private String path;
    private boolean negative = false;
    private boolean strictMode = false; // default is false
    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;
    private int timeout = Config.stream_load_default_timeout_second;
    private long execMemLimit = 0;
    private long loadMemLimit = 0;
    private boolean partialUpdate = false;
    private TCompressionType compressionType = TCompressionType.NO_COMPRESSION;
    private int loadParallelRequestNum = 0;
    private boolean enableReplicatedStorage = false;
    private String confluentSchemaRegistryUrl;
    private long logRejectedRecordNum = 0;
    private TPartialUpdateMode partialUpdateMode = TPartialUpdateMode.ROW_MODE;
    private long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;

    private TCompressionType payloadCompressionType = TCompressionType.NO_COMPRESSION;

    public StreamLoadInfo(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
    }

    public StreamLoadInfo(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType, Optional<Integer> timeout) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
        timeout.ifPresent(integer -> this.timeout = integer);
    }

    public String getConfluentSchemaRegistryUrl() {
        return confluentSchemaRegistryUrl;
    }

    public void setConfluentSchemaRegistryUrl(String confluentSchemaRegistryUrl) {
        this.confluentSchemaRegistryUrl = confluentSchemaRegistryUrl;
    }

    public TUniqueId getId() {
        return id;
    }

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public TFileType getFileType() {
        return fileType;
    }

    public TFileFormatType getFormatType() {
        return formatType;
    }

    public List<ImportColumnDesc> getColumnExprDescs() {
        return columnExprDescs;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public String getMergeConditionStr() {
        return mergeConditionStr;
    }

    public TPartialUpdateMode getPartialUpdateMode() {
        return partialUpdateMode;
    }

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public boolean getTrimSpace() {
        return trimSpace;
    }

    public long getSkipHeader() {
        return skipHeader;
    }

    public byte getEnclose() {
        return enclose;
    }

    public byte getEscape() {
        return escape;
    }

    public PartitionNames getPartitions() {
        return partitions;
    }

    public boolean isSpecifiedPartitions() {
        return partitions != null;
    }

    public String getPath() {
        return path;
    }

    public boolean getNegative() {
        return negative;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public String getTimezone() {
        return timezone;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isStripOuterArray() {
        return stripOuterArray;
    }

    public void setStripOuterArray(boolean stripOuterArray) {
        this.stripOuterArray = stripOuterArray;
    }

    public String getJsonPaths() {
        return jsonPaths;
    }

    public void setJsonPath(String jsonPaths) {
        this.jsonPaths = jsonPaths;
    }

    public String getJsonRoot() {
        return jsonRoot;
    }

    public void setJsonRoot(String jsonRoot) {
        this.jsonRoot = jsonRoot;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    public void setPartialUpdate(boolean partialUpdate) {
        this.partialUpdate = partialUpdate;
    }

    public TCompressionType getTransmisionCompressionType() {
        return compressionType;
    }

    public TCompressionType getPayloadCompressionType() {
        return payloadCompressionType;
    }

    public boolean getEnableReplicatedStorage() {
        return enableReplicatedStorage;
    }

    public int getLoadParallelRequestNum() {
        return loadParallelRequestNum;
    }

    public long getLogRejectedRecordNum() {
        return logRejectedRecordNum;
    }

    public void setLogRejectedRecordNum(long logRejectedRecordNum) {
        this.logRejectedRecordNum = logRejectedRecordNum;
    }

    public void setWarehouseId(long warehouseId) {
        this.warehouseId = warehouseId;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public static StreamLoadInfo fromHttpStreamLoadRequest(
            TUniqueId id, long txnId, Optional<Integer> timeout, StreamLoadKvParams params)
            throws StarRocksException {
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(id, txnId,
                params.getFileType().orElse(TFileType.FILE_STREAM),
                params.getFileFormatType().orElse(TFileFormatType.FORMAT_CSV_PLAIN), timeout);
        streamLoadInfo.setOptionalFromStreamLoad(params);
        String warehouseName = params.getWarehouse().orElse(DEFAULT_WAREHOUSE_NAME);
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
        streamLoadInfo.setWarehouseId(warehouse.getId());
        return streamLoadInfo;
    }

    public static StreamLoadInfo fromTStreamLoadPutRequest(TStreamLoadPutRequest request, Database db)
            throws StarRocksException {
        StreamLoadThriftParams streamLoadParams = new StreamLoadThriftParams(request);
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(request.getLoadId(), request.getTxnId(),
                streamLoadParams.getFileType().orElse(null), streamLoadParams.getFileFormatType().orElse(null));
        streamLoadInfo.setOptionalFromStreamLoad(streamLoadParams);
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        if (request.isSetBackend_id()) {
            SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            warehouseId = com.starrocks.lake.Utils.getWarehouseIdByNodeId(systemInfo, request.getBackend_id())
                    .orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        } else if (request.getWarehouse() != null && !request.getWarehouse().isEmpty()) {
            // For backward, we keep this else branch. We should prioritize using the method to get the warehouse by backend.
            String warehouseName = request.getWarehouse();
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
            warehouseId = warehouse.getId();
        }
        streamLoadInfo.setWarehouseId(warehouseId);
        return streamLoadInfo;
    }

    private void setOptionalFromStreamLoad(StreamLoadParams params) throws StarRocksException {
        Optional<String> columns = params.getColumns();
        if (columns.isPresent()) {
            setColumnToColumnExpr(columns.get());
        }
        Optional<String> where = params.getWhere();
        if (where.isPresent()) {
            setWhereExpr(where.get());
        }
        params.getColumnSeparator().ifPresent(value -> columnSeparator = new ColumnSeparator(value));
        params.getRowDelimiter().ifPresent(value -> rowDelimiter = new RowDelimiter(value));
        params.getSkipHeader().ifPresent(value -> skipHeader = value);
        params.getEnclose().ifPresent(value -> enclose = value);
        params.getEscape().ifPresent(value -> escape = value);
        params.getTrimSpace().ifPresent(value -> trimSpace = value);

        Optional<String> parts = params.getPartitions();
        if (parts.isPresent()) {
            String[] partNames = PART_NAME_SPLIT.split(parts.get().trim());
            Optional<Boolean> isTempPartition = params.getIsTempPartition();
            if (isTempPartition.isPresent()) {
                partitions = new PartitionNames(isTempPartition.get(), Lists.newArrayList(partNames));
            } else {
                partitions = new PartitionNames(false, Lists.newArrayList(partNames));
            }
        }

        if (fileType != null) {
            if (fileType == TFileType.FILE_STREAM) {
                path = params.getFilePath().orElse(null);
            } else {
                throw new StarRocksException("Unsupported file type, type=" + fileType);
            }
        }

        params.getNegative().ifPresent(value -> negative = value);
        params.getTimeout().ifPresent(value -> timeout = value);
        params.getStrictMode().ifPresent(value -> strictMode = value);
        Optional<String> timezoneOptional = params.getTimezone();

        if (timezoneOptional.isPresent()) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(timezoneOptional.get());
        }

        params.getLoadMemLimit().ifPresent(value -> loadMemLimit = value);

        if (formatType == TFileFormatType.FORMAT_JSON) {
            params.getJsonPaths().ifPresent(value -> jsonPaths = value);
            params.getJsonRoot().ifPresent(value -> jsonRoot = value);
            params.getStripOuterArray().ifPresent(value -> stripOuterArray = value);
        }

        params.getTransmissionCompressionType().ifPresent(
                value -> compressionType = CompressionUtils.findTCompressionByName(value));
        params.getLoadDop().ifPresent(value -> loadParallelRequestNum = value);
        params.getEnableReplicatedStorage().ifPresent(value -> enableReplicatedStorage = value);
        params.getMergeCondition().ifPresent(value -> mergeConditionStr = value);
        params.getLogRejectedRecordNum().ifPresent(value -> logRejectedRecordNum = value);
        params.getPartialUpdate().ifPresent(value -> partialUpdate = value);
        params.getPartialUpdateMode().ifPresent(value -> partialUpdateMode = value);

        Optional<String> compressionType = params.getPayloadCompressionType();
        if (compressionType.isPresent()) {
            payloadCompressionType = CompressionUtils.findTCompressionByName(compressionType.get());
            if (payloadCompressionType == null) {
                throw new StarRocksException("Unsupported compression type: " + compressionType.get());
            }
        }
    }

    public static StreamLoadInfo fromRoutineLoadJob(RoutineLoadJob routineLoadJob) throws StarRocksException {
        TUniqueId dummyId = new TUniqueId();
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        if (routineLoadJob.getFormat().equals("json")) {
            fileFormatType = TFileFormatType.FORMAT_JSON;
        }
        if (routineLoadJob.getFormat().equals("avro")) {
            fileFormatType = TFileFormatType.FORMAT_AVRO;
        }
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(dummyId, -1L /* dummy txn id */,
                TFileType.FILE_STREAM, fileFormatType);
        streamLoadInfo.setOptionalFromRoutineLoadJob(routineLoadJob);
        streamLoadInfo.setLogRejectedRecordNum(routineLoadJob.getLogRejectedRecordNum());
        return streamLoadInfo;
    }

    private void setOptionalFromRoutineLoadJob(RoutineLoadJob routineLoadJob) throws StarRocksException {
        // copy the columnExprDescs, cause it may be changed when planning.
        // so we keep the columnExprDescs in routine load job as origin.
        if (routineLoadJob.getColumnDescs() != null) {
            columnExprDescs = Lists.newArrayList(routineLoadJob.getColumnDescs());
        }
        whereExpr = routineLoadJob.getWhereExpr();
        setMergeConditionExpr(routineLoadJob.getMergeCondition());
        columnSeparator = routineLoadJob.getColumnSeparator();
        rowDelimiter = routineLoadJob.getRowDelimiter();
        partitions = routineLoadJob.getPartitions();
        strictMode = routineLoadJob.isStrictMode();
        timezone = routineLoadJob.getTimezone();
        timeout = (int) routineLoadJob.getTaskTimeoutSecond();
        if (!routineLoadJob.getJsonPaths().isEmpty()) {
            jsonPaths = routineLoadJob.getJsonPaths();
        }
        if (!routineLoadJob.getJsonRoot().isEmpty()) {
            jsonRoot = routineLoadJob.getJsonRoot();
        }
        stripOuterArray = routineLoadJob.isStripOuterArray();
        partialUpdate = routineLoadJob.isPartialUpdate();
        partialUpdateMode = TPartialUpdateMode.ROW_MODE;
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.EXEC_MEM_LIMIT)) {
            execMemLimit = Long.parseLong(routineLoadJob.getSessionVariables().get(SessionVariable.EXEC_MEM_LIMIT));
        } else {
            execMemLimit = SessionVariable.DEFAULT_EXEC_MEM_LIMIT;
        }
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
            compressionType = CompressionUtils.findTCompressionByName(
                    routineLoadJob.getSessionVariables().get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
        }
        confluentSchemaRegistryUrl = routineLoadJob.getConfluentSchemaRegistryUrl();
        trimSpace = routineLoadJob.isTrimspace();
        enclose = routineLoadJob.getEnclose();
        escape = routineLoadJob.getEscape();
        warehouseId = routineLoadJob.getWarehouseId();
    }

    // used for stream load
    private void setColumnToColumnExpr(String columns) throws StarRocksException {
        String columnsSQL = "COLUMNS (" + columns + ")";
        ImportColumnsStmt columnsStmt;
        try {
            columnsStmt = com.starrocks.sql.parser.SqlParser.parseImportColumns(columnsSQL, SqlModeHelper.MODE_DEFAULT);
        } catch (ParsingException e) {
            LOG.warn("parse columns' statement failed, sql={}, error={}", columnsSQL, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to parse columns header, sql={}", columnsSQL, e);
            throw new StarRocksException("parse columns header failed", e);
        }

        if (columnsStmt.getColumns() != null && !columnsStmt.getColumns().isEmpty()) {
            columnExprDescs = columnsStmt.getColumns();
        }
    }

    private void setWhereExpr(String whereString) throws StarRocksException {
        ImportWhereStmt whereStmt;
        try {
            whereStmt = new ImportWhereStmt(com.starrocks.sql.parser.SqlParser.parseSqlToExpr(whereString,
                    SqlModeHelper.MODE_DEFAULT));
        } catch (ParsingException e) {
            LOG.warn("analyze where statement failed, sql={}, error={}", whereString, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to parse where header, sql={}", whereString, e);
            throw new StarRocksException("parse columns header failed", e);
        }
        whereExpr = whereStmt.getExpr();
    }

    private void setMergeConditionExpr(String mergeConditionStr) throws StarRocksException {
        this.mergeConditionStr = mergeConditionStr;
        // TODO:(caneGuy) use expr for update condition
    }

    public long getLoadMemLimit() {
        return loadMemLimit;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }
}
