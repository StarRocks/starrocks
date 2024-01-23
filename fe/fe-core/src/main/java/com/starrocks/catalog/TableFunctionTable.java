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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.proto.PGetFileSchemaResult;
import com.starrocks.proto.PSlotDescriptor;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PGetFileSchemaRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TBrokerRangeDesc;
import com.starrocks.thrift.TBrokerScanRange;
import com.starrocks.thrift.TBrokerScanRangeParams;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TFileFormatType;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static com.google.common.base.Verify.verify;
import static com.starrocks.analysis.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;

public class TableFunctionTable extends Table {

    private static final int DEFAULT_AUTO_DETECT_SAMPLE_FILES = 1;

    private static final Logger LOG = LogManager.getLogger(TableFunctionTable.class);

    public static final String FAKE_PATH = "fake://";
    public static final String PROPERTY_PATH = "path";
    public static final String PROPERTY_FORMAT = "format";

    public static final String PROPERTY_COLUMNS_FROM_PATH = "columns_from_path";

    public static final String PROPERTY_AUTO_DETECT_SAMPLE_FILES = "auto_detect_sample_files";

    private String path;
    private String format;
    private String compressionType;

    private int autoDetectSampleFiles;

    private List<String> columnsFromPath = new ArrayList<>();
    private final Map<String, String> properties;
    @Nullable
    private List<Integer> partitionColumnIDs;
    private boolean writeSingleFile;

    private List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();

    public TableFunctionTable(Map<String, String> properties) throws DdlException {
        super(TableType.TABLE_FUNCTION);
        super.setId(-1);
        super.setName("table_function_table");
        this.properties = properties;

        parseProperties();
        parseFiles();


        List<Column> columns = new ArrayList<>();
        if (path.startsWith(FAKE_PATH)) {
            columns.add(new Column("col_int", Type.INT));
            columns.add(new Column("col_string", Type.VARCHAR));
        } else {
            columns = getFileSchema();
        }

        columns.addAll(getSchemaFromPath());

        setNewFullSchema(columns);
    }

    // Ctor for unload data via table function
    public TableFunctionTable(String path, String format, String compressionType, List<Column> columns,
                              @Nullable List<Integer> partitionColumnIDs, boolean writeSingleFile,
                              Map<String, String> properties) {
        super(TableType.TABLE_FUNCTION);
        verify(!Strings.isNullOrEmpty(path), "path is null or empty");
        verify(!(partitionColumnIDs != null && writeSingleFile));
        this.path = path;
        this.format = format;
        this.compressionType = compressionType;
        this.partitionColumnIDs = partitionColumnIDs;
        this.writeSingleFile = writeSingleFile;
        this.properties = properties;
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
        tTableFunctionTable.setCompression_type(PARQUET_COMPRESSION_TYPE_MAP.get(compressionType));
        if (partitionColumnIDs != null) {
            tTableFunctionTable.setPartition_column_ids(partitionColumnIDs);
        }
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

        if (!format.equalsIgnoreCase("parquet") && !format.equalsIgnoreCase("orc")) {
            throw new DdlException("not supported format: " + format);
        }

        String colsFromPathProp = properties.get(PROPERTY_COLUMNS_FROM_PATH);
        if (!Strings.isNullOrEmpty(colsFromPathProp)) {
            String[] colsFromPath = colsFromPathProp.split(",");
            for (String col : colsFromPath) {
                columnsFromPath.add(col.trim());
            }
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
            String errorMsg = "failed to parse files. You may give a wrong url, please check it. " + path;
            throw new DdlException(errorMsg, e);
        }

        if (fileStatuses.isEmpty()) {
            throw new DdlException("no file found with given path pattern: " + path);
        }
    }

    private PGetFileSchemaRequest getGetFileSchemaRequest(List<TBrokerFileStatus> filelist) throws TException {
        TBrokerScanRangeParams params = new TBrokerScanRangeParams();
        params.setUse_broker(false);
        params.setSrc_slot_ids(new ArrayList<>());
        params.setProperties(properties);
        params.setSchema_sample_file_count(autoDetectSampleFiles);

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

        TFileFormatType fileFormat;
        switch (format.toLowerCase()) {
            case "parquet":
                fileFormat = TFileFormatType.FORMAT_PARQUET;
                break;
            case "orc":
                fileFormat = TFileFormatType.FORMAT_ORC;
                break;
            default:
                throw new TException("unsupported format: " + format);
        }

        for (int i = 0; i < filelist.size(); ++i) {
            TBrokerRangeDesc rangeDesc = new TBrokerRangeDesc();
            rangeDesc.setFile_type(TFileType.FILE_BROKER);
            rangeDesc.setFormat_type(fileFormat);
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
        List<Long> nodeIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
        if (RunMode.isSharedDataMode()) {
            nodeIds.addAll(GlobalStateMgr.getCurrentSystemInfo().getComputeNodeIds(true));
        }
        if (nodeIds.isEmpty()) {
            if (RunMode.isSharedNothingMode()) {
                throw new DdlException("Failed to send proxy request. No alive backends");
            } else {
                throw new DdlException("Failed to send proxy request. No alive backends or compute nodes");
            }
        }

        Collections.shuffle(nodeIds);
        ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeIds.get(0));
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
        if (partitionColumnIDs == null) {
            return new ArrayList<>();
        }
        return partitionColumnIDs.stream().map(id -> fullSchema.get(id).getName()).collect(Collectors.toList());
    }

    public List<Integer> getPartitionColumnIDs() {
        if (partitionColumnIDs == null) {
            return new ArrayList<>();
        }
        return Collections.unmodifiableList(partitionColumnIDs);
    }

    public boolean isWriteSingleFile() {
        return writeSingleFile;
    }
}
