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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Delimiter;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.CsvFormat;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.CompressionUtils;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.Load;
import com.starrocks.proto.PGetFileSchemaResult;
import com.starrocks.proto.PSlotDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PGetFileSchemaRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TGetFileSchemaRequest;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableFunctionTable;
import com.starrocks.thrift.TTableType;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableFunctionTable extends Table {
    public static final String PARQUET = "parquet";
    public static final String ORC = "orc";
    public static final String CSV = "csv";

    public static final Set<String> SUPPORTED_FORMATS;
    static {
        SUPPORTED_FORMATS = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        SUPPORTED_FORMATS.add(PARQUET);
        SUPPORTED_FORMATS.add(ORC);
        SUPPORTED_FORMATS.add(CSV);
    }

    private static final int DEFAULT_AUTO_DETECT_SAMPLE_FILES = 1;
    private static final int DEFAULT_AUTO_DETECT_SAMPLE_ROWS = 500;

    private static final Logger LOG = LogManager.getLogger(TableFunctionTable.class);

    public static final String FAKE_PATH = "fake://";
    public static final String PROPERTY_PATH = "path";
    public static final String PROPERTY_FORMAT = "format";
    public static final String PROPERTY_COMPRESSION = "compression";
    public static final String PROPERTY_TARGET_MAX_FILE_SIZE = "target_max_file_size";
    public static final String PROPERTY_SINGLE = "single";
    public static final String PROPERTY_PARTITION_BY = "partition_by";

    public static final String PROPERTY_COLUMNS_FROM_PATH = "columns_from_path";
    private static final String PROPERTY_STRICT_MODE = LoadStmt.STRICT_MODE;

    public static final String PROPERTY_AUTO_DETECT_SAMPLE_FILES = "auto_detect_sample_files";
    public static final String PROPERTY_AUTO_DETECT_SAMPLE_ROWS = "auto_detect_sample_rows";

    public static final String PROPERTY_CSV_COLUMN_SEPARATOR = "csv.column_separator";
    public static final String PROPERTY_CSV_ROW_DELIMITER = "csv.row_delimiter";
    public static final String PROPERTY_CSV_SKIP_HEADER = "csv.skip_header";
    public static final String PROPERTY_CSV_ENCLOSE = "csv.enclose";
    public static final String PROPERTY_CSV_ESCAPE = "csv.escape";
    public static final String PROPERTY_CSV_TRIM_SPACE = "csv.trim_space";
    public static final String PROPERTY_PARQUET_USE_LEGACY_ENCODING = "parquet.use_legacy_encoding";

    private String path;
    private String format;
    private String compressionType;

    private int autoDetectSampleFiles;
    private int autoDetectSampleRows;

    private List<String> columnsFromPath = new ArrayList<>();
    private boolean strictMode = false;
    private final Map<String, String> properties;

    private Optional<List<Integer>> partitionColumnIDs = Optional.empty();
    private boolean writeSingleFile;
    private long targetMaxFileSize;

    // CSV format options
    private String csvColumnSeparator = "\t";
    private String csvRowDelimiter = "\n";
    private byte csvEnclose;
    private byte csvEscape;
    private long csvSkipHeader;
    private boolean csvTrimSpace;

    // PARQUET format options
    private boolean parquetUseLegacyEncoding = false;

    private List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();

    public TableFunctionTable(Map<String, String> properties) throws DdlException {
        this(properties, null);
    }

    // Ctor for load data via table function
    public TableFunctionTable(Map<String, String> properties, Consumer<TableFunctionTable> pushDownSchemaFunc)
            throws DdlException {
        super(TableType.TABLE_FUNCTION);
        super.setId(-1);
        super.setName("table_function_table");
        this.properties = properties;

        parseProperties();
        parseFiles();

        // infer schema from files
        List<Column> columns = new ArrayList<>();
        if (path.startsWith(FAKE_PATH)) {
            columns.add(new Column("col_int", Type.INT));
            columns.add(new Column("col_string", Type.VARCHAR));
        } else {
            columns = getFileSchema();
        }

        columns.addAll(getSchemaFromPath());

        setNewFullSchema(columns);

        if (pushDownSchemaFunc != null) {
            pushDownSchemaFunc.accept(this);
        }
    }

    // Ctor for unload data via table function
    public TableFunctionTable(List<Column> columns, Map<String, String> properties, SessionVariable sessionVariable) {
        super(TableType.TABLE_FUNCTION);
        checkNotNull(properties, "properties is null");
        checkNotNull(sessionVariable, "sessionVariable is null");
        this.properties = properties;
        parsePropertiesForUnload(columns, sessionVariable);
        super.setNewFullSchema(columns);
    }

    @Override
    public boolean supportInsert() {
        return true;
    }

    public List<TBrokerFileStatus> fileList() {
        return fileStatuses;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.TABLE_FUNCTION_TABLE, fullSchema.size(),
                0, "_table_function_table", "_table_function_db");

        TTableFunctionTable tTableFunctionTable = this.toTTableFunctionTable();
        tTableDescriptor.setTableFunctionTable(tTableFunctionTable);
        return tTableDescriptor;
    }

    public TTableFunctionTable toTTableFunctionTable() {
        TTableFunctionTable tTableFunctionTable = new TTableFunctionTable();
        List<TColumn> tColumns = getFullSchema().stream().map(Column::toThrift).collect(Collectors.toList());
        tTableFunctionTable.setPath(path);
        tTableFunctionTable.setColumns(tColumns);
        tTableFunctionTable.setFile_format(format);
        tTableFunctionTable.setWrite_single_file(writeSingleFile);
        Preconditions.checkState(CompressionUtils.getConnectorSinkCompressionType(compressionType).isPresent());
        tTableFunctionTable.setCompression_type(CompressionUtils.getConnectorSinkCompressionType(compressionType).get());
        tTableFunctionTable.setTarget_max_file_size(targetMaxFileSize);
        if (CSV.equalsIgnoreCase(format)) {
            tTableFunctionTable.setCsv_column_seperator(csvColumnSeparator);
            tTableFunctionTable.setCsv_row_delimiter(csvRowDelimiter);
        }
        tTableFunctionTable.setParquet_use_legacy_encoding(parquetUseLegacyEncoding);
        partitionColumnIDs.ifPresent(tTableFunctionTable::setPartition_column_ids);
        return tTableFunctionTable;
    }

    public String getFormat() {
        return format;
    }

    public String getPath() {
        return path;
    }

    private void parseProperties() throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of table function");
        }

        path = properties.get(PROPERTY_PATH);
        if (Strings.isNullOrEmpty(path)) {
            throw new DdlException("path is null. Please add properties(path='xxx') when create table");
        }

        format = properties.get(PROPERTY_FORMAT);
        if (Strings.isNullOrEmpty(format)) {
            throw new DdlException("format is null. Please add properties(format='xxx') when create table");
        }

        if (!SUPPORTED_FORMATS.contains(format.toLowerCase())) {
            throw new DdlException("not supported format: " + format);
        }

        String colsFromPathProp = properties.get(PROPERTY_COLUMNS_FROM_PATH);
        if (!Strings.isNullOrEmpty(colsFromPathProp)) {
            String[] colsFromPath = colsFromPathProp.split(",");
            for (String col : colsFromPath) {
                columnsFromPath.add(col.trim());
            }
        }

        if (properties.containsKey(PROPERTY_STRICT_MODE)) {
            strictMode = Boolean.parseBoolean(properties.get(PROPERTY_STRICT_MODE));
        }

        if (!properties.containsKey(PROPERTY_AUTO_DETECT_SAMPLE_FILES)) {
            autoDetectSampleFiles = DEFAULT_AUTO_DETECT_SAMPLE_FILES;
        } else {
            try {
                autoDetectSampleFiles = Integer.parseInt(properties.get(PROPERTY_AUTO_DETECT_SAMPLE_FILES));
            } catch (NumberFormatException e) {
                throw new DdlException("failed to parse auto_detect_sample_files: ", e);
            }
        }

        if (!properties.containsKey(PROPERTY_AUTO_DETECT_SAMPLE_ROWS)) {
            autoDetectSampleRows = DEFAULT_AUTO_DETECT_SAMPLE_ROWS;
        } else {
            try {
                autoDetectSampleRows = Integer.parseInt(properties.get(PROPERTY_AUTO_DETECT_SAMPLE_ROWS));
            } catch (NumberFormatException e) {
                throw new DdlException("failed to parse auto_detect_sample_files: ", e);
            }
        }

        if (properties.containsKey(PROPERTY_CSV_COLUMN_SEPARATOR)) {
            csvColumnSeparator = Delimiter.convertDelimiter(properties.get(PROPERTY_CSV_COLUMN_SEPARATOR));
            int len = csvColumnSeparator.getBytes(StandardCharsets.UTF_8).length;
            if (len > CsvFormat.MAX_COLUMN_SEPARATOR_LENGTH || len == 0) {
                ErrorReport.reportDdlException(ErrorCode.ERR_ILLEGAL_BYTES_LENGTH,
                        PROPERTY_CSV_COLUMN_SEPARATOR, 1, CsvFormat.MAX_COLUMN_SEPARATOR_LENGTH);
            }
        }

        if (properties.containsKey(PROPERTY_CSV_ROW_DELIMITER)) {
            csvRowDelimiter = Delimiter.convertDelimiter(properties.get(PROPERTY_CSV_ROW_DELIMITER));
            int len = csvRowDelimiter.getBytes(StandardCharsets.UTF_8).length;
            if (len > CsvFormat.MAX_ROW_DELIMITER_LENGTH || len == 0) {
                ErrorReport.reportDdlException(ErrorCode.ERR_ILLEGAL_BYTES_LENGTH,
                        PROPERTY_CSV_ROW_DELIMITER, 1, CsvFormat.MAX_ROW_DELIMITER_LENGTH);
            }
        }

        if (properties.containsKey(PROPERTY_CSV_ENCLOSE)) {
            byte[] bs = properties.get(PROPERTY_CSV_ENCLOSE).getBytes();
            if (bs.length == 0) {
                throw new DdlException("empty property csv.enclose");
            }
            csvEnclose = bs[0];
        }

        if (properties.containsKey(PROPERTY_CSV_ESCAPE)) {
            byte[] bs = properties.get(PROPERTY_CSV_ESCAPE).getBytes();
            if (bs.length == 0) {
                throw new DdlException("empty property csv.escape");
            }
            csvEscape = bs[0];
        }

        if (properties.containsKey(PROPERTY_CSV_SKIP_HEADER)) {
            try {
                csvSkipHeader = Integer.parseInt(properties.get(PROPERTY_CSV_SKIP_HEADER));
            } catch (NumberFormatException e) {
                throw new DdlException("failed to parse csv.skip_header: ", e);
            }
        }

        if (properties.containsKey(PROPERTY_CSV_TRIM_SPACE)) {
            String property = properties.get(PROPERTY_CSV_TRIM_SPACE);
            if (property.equalsIgnoreCase("true")) {
                csvTrimSpace = true;
            } else if (property.equalsIgnoreCase("false")) {
                csvTrimSpace = false;
            } else {
                throw new DdlException("illegal value of csv.trim_space: " + property + ", only true/false allowed");
            }
        }
    }

    private void parseFiles() throws DdlException {
        try {
            // fake:// is a faked path, for testing purpose
            if (path.startsWith("fake://")) {
                TBrokerFileStatus file1 = new TBrokerFileStatus();
                file1.isDir = false;
                file1.path = "fake://some_bucket/some_dir/file1";
                file1.size = 1024;
                fileStatuses.add(file1);

                TBrokerFileStatus file2 = new TBrokerFileStatus();
                file2.isDir = false;
                file2.path = "fake://some_bucket/some_dir/file2";
                file2.size = 2048;
                fileStatuses.add(file2);
                return;
            }
            List<String> pieces = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(path);
            for (String piece : ListUtils.emptyIfNull(pieces)) {
                HdfsUtil.parseFile(piece, new BrokerDesc(properties), fileStatuses);
            }
        } catch (UserException e) {
            LOG.error("parse files error", e);
            throw new DdlException("failed to parse files", e);
        }

        if (fileStatuses.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_FILES_FOUND, path);
        }
    }

    private PGetFileSchemaRequest getGetFileSchemaRequest(List<TBrokerFileStatus> filelist) throws TException {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setUse_broker(false);
        params.setSrc_slot_ids(new ArrayList<>());
        params.setProperties(properties);
        params.setSchema_sample_file_count(autoDetectSampleFiles);
        params.setSchema_sample_file_row_count(autoDetectSampleRows);
        params.setEnclose(csvEnclose);
        params.setEscape(csvEscape);
        params.setSkip_header(csvSkipHeader);
        params.setTrim_space(csvTrimSpace);
        params.setFlexible_column_mapping(true);
        if (csvColumnSeparator.getBytes(StandardCharsets.UTF_8).length == 1) {
            params.setColumn_separator(csvColumnSeparator.getBytes()[0]);
        } else {
            params.setMulti_column_separator(csvColumnSeparator);
        }

        if (csvRowDelimiter.getBytes(StandardCharsets.UTF_8).length == 1) {
            params.setRow_delimiter(csvRowDelimiter.getBytes()[0]);
        } else {
            params.setMulti_row_delimiter(csvRowDelimiter);
        }

        try {
            THdfsProperties hdfsProperties = new THdfsProperties();
            HdfsUtil.getTProperties(filelist.get(0).path, new BrokerDesc(properties), hdfsProperties);
            params.setHdfs_properties(hdfsProperties);
        } catch (UserException e) {
            throw new TException("failed to parse files: " + e.getMessage());
        }

        TBrokerScanRange brokerScanRange = new TBrokerScanRange();
        brokerScanRange.setParams(params);
        brokerScanRange.setBroker_addresses(Lists.newArrayList());

        for (int i = 0; i < filelist.size(); ++i) {
            TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
            rangeDesc.setFile_type(TFileType.FILE_BROKER);
            rangeDesc.setFormat_type(Load.getFormatType(format, filelist.get(i).path));
            rangeDesc.setPath(filelist.get(i).path);
            rangeDesc.setSplittable(filelist.get(i).isSplitable);
            rangeDesc.setStart_offset(0);
            rangeDesc.setFile_size(filelist.get(i).size);
            rangeDesc.setSize(filelist.get(i).size);
            rangeDesc.setNum_of_columns_from_file(0);
            rangeDesc.setColumns_from_path(new ArrayList<>());
            brokerScanRange.addToRanges(rangeDesc);
        }

        TScanRange scanRange = new TScanRange();
        scanRange.setBroker_scan_range(brokerScanRange);

        final TGetFileSchemaRequest tRequest = new TGetFileSchemaRequest();
        tRequest.setScan_range(scanRange);

        final PGetFileSchemaRequest pRequest = new PGetFileSchemaRequest();
        pRequest.setRequest(tRequest);
        return pRequest;
    }

    private List<Column> getFileSchema() throws DdlException {
        if (fileStatuses.isEmpty()) {
            return Lists.newArrayList();
        }
        TNetworkAddress address;
        List<Long> nodeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
        if (RunMode.isSharedDataMode()) {
            nodeIds.addAll(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodeIds(true));
        }
        if (nodeIds.isEmpty()) {
            if (RunMode.isSharedNothingMode()) {
                throw new DdlException("Failed to send proxy request. No alive backends");
            } else {
                throw new DdlException("Failed to send proxy request. No alive backends or compute nodes");
            }
        }

        Collections.shuffle(nodeIds);
        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
        address = new TNetworkAddress(node.getHost(), node.getBrpcPort());

        PGetFileSchemaResult result;
        try {
            PGetFileSchemaRequest request = getGetFileSchemaRequest(fileStatuses);
            Future<PGetFileSchemaResult> future = BackendServiceClient.getInstance().getFileSchema(address, request);
            result = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DdlException("failed to get file schema", e);
        } catch (Exception e) {
            throw new DdlException("failed to get file schema: " + e.getMessage());
        }

        if (TStatusCode.findByValue(result.status.statusCode) != TStatusCode.OK) {
            throw new DdlException("failed to get file schema, path: " + path + ", error: " + result.status.errorMsgs);
        }

        List<Column> columns = new ArrayList<>();
        for (PSlotDescriptor slot : result.schema) {
            columns.add(new Column(slot.colName, Type.fromProtobuf(slot.slotType), true));
        }
        return columns;
    }
    private List<Column> getSchemaFromPath() throws DdlException {
        List<Column> columns = new ArrayList<>();
        if (!columnsFromPath.isEmpty()) {
            for (String colName : columnsFromPath) {
                Optional<Column> column =  columns.stream().filter(col -> col.nameEquals(colName, false)).findFirst();
                if (column.isPresent()) {
                    throw new DdlException("duplicated name in columns from path, " +
                            "a column with same name already exists in the file table: " + colName);
                }
                columns.add(new Column(colName, ScalarType.createDefaultString(), true));
            }
        }
        return columns;
    }

    public List<ImportColumnDesc> getColumnExprList() {
        List<ImportColumnDesc> exprs = new ArrayList<>();
        List<Column> columns = super.getFullSchema();
        for (Column column : columns) {
            exprs.add(new ImportColumnDesc(column.getName()));
        }
        return exprs;
    }

    public List<String> getColumnsFromPath() {
        return columnsFromPath;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    @Override
    public String toString() {
        return String.format("TABLE('path'='%s', 'format'='%s')", path, format);
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partitionColumnIDs.map(integers -> integers.stream()
                .map(id -> fullSchema.get(id).getName())
                .collect(Collectors.toList()))
                .orElseGet(ArrayList::new);
    }

    public List<Integer> getPartitionColumnIDs() {
        return partitionColumnIDs.map(Collections::unmodifiableList)
                .orElseGet(ArrayList::new);
    }

    public boolean isWriteSingleFile() {
        return writeSingleFile;
    }

    public String getCsvColumnSeparator() {
        return csvColumnSeparator;
    }

    public String getCsvRowDelimiter() {
        return csvRowDelimiter;
    }

    public byte getCsvEnclose() {
        return csvEnclose;
    }

    public byte getCsvEscape() {
        return csvEscape;
    }

    public long getCsvSkipHeader() {
        return csvSkipHeader;
    }

    public Boolean getCsvTrimSpace() {
        return csvTrimSpace;
    }

    public void parsePropertiesForUnload(List<Column> columns, SessionVariable sessionVariable) {
        List<String> columnNames = columns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());

        Set<String> duplicateColumnNames = columns.stream()
                .map(Column::getName)
                .filter(name -> Collections.frequency(columnNames, name) > 1)
                .collect(Collectors.toSet());
        if (!duplicateColumnNames.isEmpty()) {
            throw new SemanticException("expect column names to be distinct, but got duplicate(s): " + duplicateColumnNames);
        }

        // parse table function properties
        String single = properties.getOrDefault(PROPERTY_SINGLE, "false");
        if (!single.equalsIgnoreCase("true") && !single.equalsIgnoreCase("false")) {
            throw new SemanticException("got invalid parameter \"single\" = \"%s\", expect a boolean value (true or false).",
                    single);
        }

        this.writeSingleFile = single.equalsIgnoreCase("true");

        // validate properties
        if (!properties.containsKey(PROPERTY_PATH)) {
            throw new SemanticException(
                    "path is a mandatory property. \"path\" = \"s3://path/to/your/location/\"");
        }
        this.path = properties.get(PROPERTY_PATH);
        if (!this.path.endsWith("/")) {
            this.path += "/";
        }

        if (!properties.containsKey(PROPERTY_FORMAT)) {
            throw new SemanticException("format is a mandatory property. " +
                    "Use any of (parquet, orc, csv)");
        }
        this.format = properties.get(PROPERTY_FORMAT);

        if (!SUPPORTED_FORMATS.contains(format)) {
            throw new SemanticException(String.format("Unsupported format %s. " +
                    "Use any of (parquet, orc, csv)", format));
        }

        // if max_file_size is not specified, use target max file size from session
        if (properties.containsKey(PROPERTY_TARGET_MAX_FILE_SIZE)) {
            this.targetMaxFileSize = Long.parseLong(properties.get(PROPERTY_TARGET_MAX_FILE_SIZE));
        } else {
            this.targetMaxFileSize = sessionVariable.getConnectorSinkTargetMaxFileSize();
        }

        // if compression codec is not specified, use compression codec from session
        if (properties.containsKey(PROPERTY_COMPRESSION)) {
            this.compressionType = properties.get(PROPERTY_COMPRESSION);
        } else {
            this.compressionType = sessionVariable.getConnectorSinkCompressionCodec();
        }
        if (CompressionUtils.getConnectorSinkCompressionType(compressionType).isEmpty()) {
            throw new SemanticException(String.format("Unsupported compression codec %s. " +
                    "Use any of (uncompressed, snappy, lz4, zstd, gzip)", compressionType));
        }

        if (writeSingleFile && properties.containsKey(PROPERTY_PARTITION_BY)) {
            throw new SemanticException("cannot use partition_by and single simultaneously.");
        }

        if (properties.containsKey(PROPERTY_PARTITION_BY)) {
            // parse and validate partition columns
            List<String> partitionColumnNames = Arrays.asList(properties.get(PROPERTY_PARTITION_BY).split(","));
            partitionColumnNames.replaceAll(String::trim);
            partitionColumnNames = partitionColumnNames.stream().distinct().collect(Collectors.toList());

            List<String> unmatchedPartitionColumnNames = partitionColumnNames.stream()
                    .filter(col -> !columnNames.contains(col))
                    .collect(Collectors.toList());
            if (!unmatchedPartitionColumnNames.isEmpty()) {
                throw new SemanticException("partition columns expected to be a subset of " + columnNames +
                        ", but got extra columns: " + unmatchedPartitionColumnNames);
            }

            List<Integer> partitionColumnIDs = partitionColumnNames.stream()
                    .map(columnNames::indexOf)
                    .collect(Collectors.toList());

            for (Integer partitionColumnID : partitionColumnIDs) {
                Column partitionColumn = columns.get(partitionColumnID);
                Type type = partitionColumn.getType();
                if (type.isBoolean() || type.isIntegerType() || type.isDateType() || type.isStringType()) {
                    continue;
                }
                throw new SemanticException("partition column does not support type of " + type);
            }

            this.partitionColumnIDs = Optional.of(partitionColumnIDs);
        }

        // csv options
        if (properties.containsKey(PROPERTY_CSV_COLUMN_SEPARATOR)) {
            this.csvColumnSeparator = properties.get(PROPERTY_CSV_COLUMN_SEPARATOR);
        }
        if (properties.containsKey(PROPERTY_CSV_ROW_DELIMITER)) {
            this.csvRowDelimiter = properties.get(PROPERTY_CSV_ROW_DELIMITER);
        }

        // parquet options
        if (properties.containsKey(PROPERTY_PARQUET_USE_LEGACY_ENCODING)) {
            String useLegacyEncoding = properties.getOrDefault(PROPERTY_PARQUET_USE_LEGACY_ENCODING, "false");
            if (!useLegacyEncoding.equalsIgnoreCase("true") && !useLegacyEncoding.equalsIgnoreCase("false")) {
                throw new SemanticException("got invalid parameter \"parquet.use_legacy_encoding\" = \"%s\", " +
                        "expect a boolean value (true or false).", useLegacyEncoding);
            }
            this.parquetUseLegacyEncoding = useLegacyEncoding.equalsIgnoreCase("true");
        }
    }
}
