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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.proto.PGetFileSchemaResult;
import com.starrocks.proto.PScalarType;
import com.starrocks.proto.PTypeDesc;
import com.starrocks.proto.PTypeNode;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PGetFileSchemaRequest;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.system.Backend;
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
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTempExtTable;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class TempExternalTable extends Table {

    private static final String PROPERTY_PATH = "path";
    private static final String PROPERTY_FORMAT = "format";

    private String path;
    private String format;
    private Map<String, String> properties;

    private List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();

    public TempExternalTable(Map<String, String> properties) throws DdlException {
        super(TableType.TEMPORARY_EXTERNAL_TABLE);
        super.setId(-1);
        super.setName("temp_external_table");
        this.properties = properties;
        parseProperties();

        parseFiles();

        super.setNewFullSchema(getFileSchema());
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
        TTempExtTable tTbl = new TTempExtTable();
        tTbl.setLocation(path);

        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getBaseSchema()) {
            tColumns.add(column.toThrift());
        }
        tTbl.setColumns(tColumns);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.FILE_TABLE, fullSchema.size(),
                0, "", "");
        tTableDescriptor.setTempExtTable(tTbl);
        return tTableDescriptor;
    }

    public String getFormat() {
        return format;
    }

    public String getPath() {
        return path;
    }

    private void parseProperties() throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of external table");
        }

        path = properties.get(PROPERTY_PATH);
        if (Strings.isNullOrEmpty(path)) {
            throw new DdlException("path is null. Please add properties(path='xxx') when create table");
        }

        format = properties.get(PROPERTY_FORMAT);
        if (Strings.isNullOrEmpty(format)) {
            throw new DdlException("format is null. Please add properties(format='xxx') when create table");
        }

        if (!format.equalsIgnoreCase("parquet")) {
            throw new DdlException("not supported format: " + format);
        }
    }

    private void parseFiles() throws DdlException {
        try {
            HdfsUtil.parseFile(path, new BrokerDesc(properties), fileStatuses);
        } catch (UserException e) {
            throw new DdlException("failed to parse files: " + e.getMessage());
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
            rangeDesc.setFormat_type(TFileFormatType.FORMAT_PARQUET);
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
        TNetworkAddress address;
        try {
            List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new DdlException("Failed to send proxy request. No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendIds.get(0));
            address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            // TODO(fw): more format support.
            PGetFileSchemaRequest request = getGetFileSchemaRequest(fileStatuses);

            Future<PGetFileSchemaResult> future = BackendServiceClient.getInstance().getFileSchema(address, request);
            PGetFileSchemaResult result = future.get();

            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < result.columnNames.size(); ++i) {
                PTypeDesc typeDesc  = result.columnTypes.get(i);
                if (typeDesc.types.size() != 1) {
                    throw new DdlException("unsupported column type");
                }

                for (PTypeNode typeNode : typeDesc.types) {
                    PScalarType scalarType = typeNode.scalarType;
                    TPrimitiveType tPrimitiveType = TPrimitiveType.findByValue(scalarType.type);
                    columns.add(new Column(result.columnNames.get(i),
                            ScalarType.createType(PrimitiveType.fromThrift(tPrimitiveType)), true));
                }
            }
            return columns;
        } catch (Exception e) {
            throw new DdlException("failed to get file schema: " + e.getMessage());
        }
    }

    public List<ImportColumnDesc> getColumnExprList() {
        List<ImportColumnDesc> exprs = new ArrayList<>();
        List<Column> columns = super.getFullSchema();
        for (Column column : columns) {
            exprs.add(new ImportColumnDesc(column.getName()));
        }
        return exprs;
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
