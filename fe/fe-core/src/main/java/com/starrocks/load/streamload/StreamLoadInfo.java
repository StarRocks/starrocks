// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.load.streamload;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ColumnSeparator;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RowDelimiter;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.ImportWhereStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class StreamLoadInfo {

    private static final Logger LOG = LogManager.getLogger(StreamLoadInfo.class);

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
    private ColumnSeparator columnSeparator;
    private RowDelimiter rowDelimiter;
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

    public StreamLoadInfo(TUniqueId id, long txnId, TFileType fileType, TFileFormatType formatType) {
        this.id = id;
        this.txnId = txnId;
        this.fileType = fileType;
        this.formatType = formatType;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
    }

    public StreamLoadInfo(TUniqueId id, long txnId, int timeout) {
        this.id = id;
        this.txnId = txnId;
        this.jsonPaths = "";
        this.jsonRoot = "";
        this.stripOuterArray = false;
        this.timeout = timeout;
    }

    public TUniqueId getId() {
        return id;
    }

    public long getTxnId() {
        return txnId;
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

    public ColumnSeparator getColumnSeparator() {
        return columnSeparator;
    }

    public RowDelimiter getRowDelimiter() {
        return rowDelimiter;
    }

    public PartitionNames getPartitions() {
        return partitions;
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

    public boolean getEnableReplicatedStorage() {
        return enableReplicatedStorage;
    }

    public int getLoadParallelRequestNum() {
        return loadParallelRequestNum;
    }

    public static StreamLoadInfo fromStreamLoadContext(TUniqueId id, long txnId, int timeout, StreamLoadParam context)
            throws UserException {
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(id, txnId, timeout);
        streamLoadInfo.setOptionalFromStreamLoadContext(context);
        return streamLoadInfo;
    }

    private void setOptionalFromStreamLoadContext(StreamLoadParam context) throws UserException {
        if (context.columns != null) {
            setColumnToColumnExpr(context.columns);
        }
        if (context.whereExpr != null) {
            setWhereExpr(context.whereExpr);
        }
        if (context.columnSeparator != null) {
            setColumnSeparator(context.columnSeparator);
        }
        if (context.rowDelimiter != null) {
            setRowDelimiter(context.rowDelimiter);
        }
        if (context.partitions != null) {
            String[] partNames = context.partitions.trim().split("\\s*,\\s*");
            if (context.useTempPartition) {
                partitions = new PartitionNames(true, Lists.newArrayList(partNames));
            } else {
                partitions = new PartitionNames(false, Lists.newArrayList(partNames));
            }
        }
        if (context.fileType != null) {
            this.fileType = context.fileType;
        }
        if (context.formatType != null) {
            this.formatType = context.formatType;
        }
        switch (context.fileType) {
            case FILE_STREAM:
                path = context.path;
                break;
            default:
                throw new UserException("unsupported file type, type=" + context.fileType);
        }
        if (context.negative) {
            negative = context.negative;
        }
        if (context.strictMode) {
            strictMode = context.strictMode;
        }
        if (context.timezone != null) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(context.timezone);
        }
        if (context.loadMemLimit != -1) {
            loadMemLimit = context.loadMemLimit;
        }
        if (context.formatType == TFileFormatType.FORMAT_JSON) {
            if (context.jsonPaths != null) {
                jsonPaths = context.jsonPaths;
            }
            if (context.jsonRoot != null) {
                jsonRoot = context.jsonRoot;
            }
            stripOuterArray = context.stripOuterArray;
        }
        if (context.partialUpdate) {
            partialUpdate = context.partialUpdate;
        }
        if (context.transmissionCompressionType != null) {
            compressionType = CompressionUtils.findTCompressionByName(context.transmissionCompressionType);
        }
        if (context.loadDop != -1) {
            loadParallelRequestNum = context.loadDop;
        }
    }

    public static StreamLoadInfo fromTStreamLoadPutRequest(TStreamLoadPutRequest request, Database db)
            throws UserException {
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(request.getLoadId(), request.getTxnId(),
                request.getFileType(), request.getFormatType());
        streamLoadInfo.setOptionalFromTSLPutRequest(request, db);
        return streamLoadInfo;
    }

    private void setOptionalFromTSLPutRequest(TStreamLoadPutRequest request, Database db) throws UserException {
        if (request.isSetColumns()) {
            setColumnToColumnExpr(request.getColumns());
        }
        if (request.isSetWhere()) {
            setWhereExpr(request.getWhere());
        }
        if (request.isSetColumnSeparator()) {
            setColumnSeparator(request.getColumnSeparator());
        }
        if (request.isSetRowDelimiter()) {
            setRowDelimiter(request.getRowDelimiter());
        }
        if (request.isSetPartitions()) {
            String[] partNames = request.getPartitions().trim().split("\\s*,\\s*");
            if (request.isSetIsTempPartition()) {
                partitions = new PartitionNames(request.isIsTempPartition(), Lists.newArrayList(partNames));
            } else {
                partitions = new PartitionNames(false, Lists.newArrayList(partNames));
            }
        }
        switch (request.getFileType()) {
            case FILE_STREAM:
                path = request.getPath();
                break;
            default:
                throw new UserException("unsupported file type, type=" + request.getFileType());
        }
        if (request.isSetNegative()) {
            negative = request.isNegative();
        }
        if (request.isSetTimeout()) {
            timeout = request.getTimeout();
        }
        if (request.isSetStrictMode()) {
            strictMode = request.isStrictMode();
        }
        if (request.isSetTimezone()) {
            timezone = TimeUtils.checkTimeZoneValidAndStandardize(request.getTimezone());
        }
        if (request.isSetLoadMemLimit()) {
            loadMemLimit = request.getLoadMemLimit();
        }
        if (request.getFormatType() == TFileFormatType.FORMAT_JSON) {
            if (request.getJsonpaths() != null) {
                jsonPaths = request.getJsonpaths();
            }
            if (request.getJson_root() != null) {
                jsonRoot = request.getJson_root();
            }
            stripOuterArray = request.isStrip_outer_array();
        }
        if (request.isSetPartial_update()) {
            partialUpdate = request.isPartial_update();
        }
        if (request.isSetTransmission_compression_type()) {
            compressionType = CompressionUtils.findTCompressionByName(request.getTransmission_compression_type());
        }
        if (request.isSetLoad_dop()) {
            loadParallelRequestNum = request.getLoad_dop();
        }
    }

    public static StreamLoadInfo fromRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        TUniqueId dummyId = new TUniqueId();
        TFileFormatType fileFormatType = TFileFormatType.FORMAT_CSV_PLAIN;
        if (routineLoadJob.getFormat().equals("json")) {
            fileFormatType = TFileFormatType.FORMAT_JSON;
        }
        StreamLoadInfo streamLoadInfo = new StreamLoadInfo(dummyId, -1L /* dummy txn id */,
                TFileType.FILE_STREAM, fileFormatType);
        streamLoadInfo.setOptionalFromRoutineLoadJob(routineLoadJob);
        return streamLoadInfo;
    }

    private void setOptionalFromRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        // copy the columnExprDescs, cause it may be changed when planning.
        // so we keep the columnExprDescs in routine load job as origin.
        if (routineLoadJob.getColumnDescs() != null) {
            columnExprDescs = Lists.newArrayList(routineLoadJob.getColumnDescs());
        }
        whereExpr = routineLoadJob.getWhereExpr();
        columnSeparator = routineLoadJob.getColumnSeparator();
        rowDelimiter = routineLoadJob.getRowDelimiter();
        partitions = routineLoadJob.getPartitions();
        strictMode = routineLoadJob.isStrictMode();
        timezone = routineLoadJob.getTimezone();
        timeout = (int) Config.routine_load_task_timeout_second;
        if (!routineLoadJob.getJsonPaths().isEmpty()) {
            jsonPaths = routineLoadJob.getJsonPaths();
        }
        if (!routineLoadJob.getJsonRoot().isEmpty()) {
            jsonRoot = routineLoadJob.getJsonRoot();
        }
        stripOuterArray = routineLoadJob.isStripOuterArray();
        partialUpdate = routineLoadJob.isPartialUpdate();
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.EXEC_MEM_LIMIT)) {
            execMemLimit = Long.parseLong(routineLoadJob.getSessionVariables().get(SessionVariable.EXEC_MEM_LIMIT));
        } else {
            execMemLimit = SessionVariable.DEFAULT_EXEC_MEM_LIMIT;
        }
        if (routineLoadJob.getSessionVariables().containsKey(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE)) {
            compressionType = CompressionUtils.findTCompressionByName(
                    routineLoadJob.getSessionVariables().get(SessionVariable.LOAD_TRANSMISSION_COMPRESSION_TYPE));
        }
    }

    // used for stream load
    private void setColumnToColumnExpr(String columns) throws UserException {
        String columnsSQL = "COLUMNS (" + columns + ")";
        ImportColumnsStmt columnsStmt;
        try {
            columnsStmt = com.starrocks.sql.parser.SqlParser.parseImportColumns(columnsSQL, SqlModeHelper.MODE_DEFAULT);
        } catch (ParsingException e) {
            LOG.warn("parse columns' statement failed, sql={}, error={}", columnsSQL, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to parse columns header, sql={}", columnsSQL, e);
            throw new UserException("parse columns header failed", e);
        }

        if (columnsStmt.getColumns() != null && !columnsStmt.getColumns().isEmpty()) {
            columnExprDescs = columnsStmt.getColumns();
        }
    }

    private void setWhereExpr(String whereString) throws UserException {
        ImportWhereStmt whereStmt;
        try {
            whereStmt = new ImportWhereStmt(com.starrocks.sql.parser.SqlParser.parseSqlToExpr(whereString,
                    SqlModeHelper.MODE_DEFAULT));
        } catch (ParsingException e) {
            LOG.warn("analyze where statement failed, sql={}, error={}", whereString, e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to parse where header, sql={}", whereString, e);
            throw new UserException("parse columns header failed", e);
        }
        whereExpr = whereStmt.getExpr();
    }

    private void setColumnSeparator(String oriSeparator) throws AnalysisException {
        columnSeparator = new ColumnSeparator(oriSeparator);
        columnSeparator.analyze();
    }

    private void setRowDelimiter(String orgDelimiter) throws AnalysisException {
        rowDelimiter = new RowDelimiter(orgDelimiter);
        rowDelimiter.analyze();
    }

    public long getLoadMemLimit() {
        return loadMemLimit;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }
}
