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

package com.starrocks.planner;

import com.aliyun.datalake.common.impl.Base64Util;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.util.DlfUtil;
import com.starrocks.connector.Connector;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPaimonTableSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PaimonTableSink extends DataSink {

    private static final Logger LOG = LogManager.getLogger(PaimonTableSink.class);
    protected final TupleDescriptor desc;
    private final long targetTableId;
    //private final String fileFormat;
    private final String location;
    //private final String compressionType;
    private List<String> columnNames = new ArrayList<>();
    private final List<String> columnTypes = new ArrayList<>();
    private final boolean isStaticPartitionSink;
    private final String tableIdentifier;
    private CloudConfiguration cloudConfiguration;

    public PaimonTableSink(PaimonTable paimonTable, TupleDescriptor desc, boolean isStaticPartitionSink, SessionVariable sessionVariable) {
        Table nativeTable = paimonTable.getNativeTable();
        this.location = nativeTable.name();
        this.targetTableId = paimonTable.getId();
        this.desc = desc;
        this.columnNames = ((FileStoreTable) paimonTable.getNativeTable()).rowType().getFieldNames();
        setColumnTypes(nativeTable.rowType().getFieldTypes());
        this.isStaticPartitionSink = isStaticPartitionSink;
        this.tableIdentifier = paimonTable.getUUID();
        String catalogName = paimonTable.getCatalogName();
        Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));
        this.cloudConfiguration = connector.getMetadata().getCloudConfiguration();

    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix + "Paimon TABLE SINK\n");
        strBuilder.append(prefix + "  TABLE: " + tableIdentifier + "\n");
        strBuilder.append(prefix + "  TUPLE ID: " + desc.getId() + "\n");
        strBuilder.append(prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel));
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        TPaimonTableSink tPaimonTableSink = new TPaimonTableSink();
        tPaimonTableSink.setLocation(location);
        tPaimonTableSink.setTarget_table_id(targetTableId);
        tPaimonTableSink.setIs_static_partition_sink(isStaticPartitionSink);
        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        try {
            String dataTokenPath = DlfUtil.getDataTokenPath(location);
            if (!Strings.isNullOrEmpty(dataTokenPath)) {
                dataTokenPath = "/secret/DLF/data/" + Base64Util.encodeBase64WithoutPadding(dataTokenPath);
                File dataTokenFile = new File(dataTokenPath);

                if (dataTokenFile.exists()) {
                    cloudConfiguration = CloudConfigurationFactory.buildCloudConfigurationForStorage(
                            DlfUtil.setDataToken(dataTokenFile));
                } else {
                    LOG.warn("Cannot find data token file " + dataTokenPath);
                }
            }
        } catch (Exception e) {
            LOG.warn("Fail to get data token: " + e.getMessage());
        }
        cloudConfiguration.toThrift(tCloudConfiguration);
        tPaimonTableSink.setCloud_configuration(tCloudConfiguration);
        tPaimonTableSink.setData_column_names(columnNames);
        tPaimonTableSink.setData_column_types(columnTypes);
        tDataSink.setPaimon_table_sink(tPaimonTableSink);

        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    private void setColumnTypes(List<DataType> dataTypes) {
        for (DataType dataType : dataTypes) {
            LOG.info(dataType.toString());
            if (dataType instanceof CharType) {
                columnTypes.add("CHAR");
            } else if (dataType instanceof VarCharType) {
                columnTypes.add("STRING");
            } else if (dataType instanceof TinyIntType) {
                columnTypes.add("BYTE");
            } else if (dataType instanceof SmallIntType) {
                columnTypes.add("SMALLINT");
            } else if (dataType instanceof IntType) {
                columnTypes.add("INT");
            } else if (dataType instanceof BigIntType) {
                columnTypes.add("LONG");
            } else if (dataType instanceof FloatType) {
                columnTypes.add("FLOAT");
            } else if (dataType instanceof DoubleType) {
                columnTypes.add("DOUBLE");
            } else if (dataType instanceof BooleanType) {
                columnTypes.add("BOOL");
            } else if (dataType instanceof DateType) {
                columnTypes.add("DATE");
            } else if (dataType instanceof TimeType) {
                columnTypes.add("TIME");
            } else if (dataType instanceof DecimalType) {
                columnTypes.add("DECIMAL");
            } else {
                LOG.info("data type " + dataType.toString() + " does not support write.");
            }
        }
    }
}
