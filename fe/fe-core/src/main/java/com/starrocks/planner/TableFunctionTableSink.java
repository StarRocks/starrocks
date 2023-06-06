
package com.starrocks.planner;

import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TTableFunctionTableSink;

public class TableFunctionTableSink extends DataSink {
    private final TableFunctionTable table;
    private final CloudConfiguration cloudConfiguration;

    public TableFunctionTableSink(TableFunctionTable targetTable, CloudConfiguration cloudConfiguration) {
        this.table = targetTable;
        this.cloudConfiguration = cloudConfiguration;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return prefix + "TABLE FUNCTION TABLE SINK\n" +
                prefix + "  PATH: " + table.getPath() + "\n" +
                prefix + "  FORMAT: " + table.getFormat() + "\n" +
                prefix + "  PARTITION BY: " + table.getPartitionColumnNames() + "\n" +
                prefix + "  SINGLE: " + table.isWriteSingleFile() + "\n" +
                prefix + "  " + DataPartition.RANDOM.getExplainString(explainLevel);
    }

    @Override
    protected TDataSink toThrift() {
        TTableFunctionTableSink tTableFunctionTableSink = new TTableFunctionTableSink();
        tTableFunctionTableSink.setPath(table.getPath());
        tTableFunctionTableSink.setFile_format(table.getFormat());
        // TODO: use user passed compression_codec
        tTableFunctionTableSink.setCompression_type(TCompressionType.NO_COMPRESSION);

        TCloudConfiguration tCloudConfiguration = new TCloudConfiguration();
        cloudConfiguration.toThrift(tCloudConfiguration);
        tTableFunctionTableSink.setCloud_configuration(tCloudConfiguration);
        TDataSink tDataSink = new TDataSink(TDataSinkType.TABLE_FUNCTION_TABLE_SINK);
        tDataSink.setTable_function_table_sink(tTableFunctionTableSink);
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
    public boolean canUsePipeLine() {
        return true;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }
}