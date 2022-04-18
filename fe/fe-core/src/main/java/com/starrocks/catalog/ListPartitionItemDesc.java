package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeNameFormat;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.thrift.TTabletType;
import org.apache.commons.lang.NotImplementedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ListPartitionItemDesc extends PartitionDesc {

    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private boolean isInMemory;
    private TTabletType tabletType;
    private Long versionInfo;

    public String getPartitionName(){
        throw new NotImplementedException();
    }

    public Map<String, String> getPartitionProperties() {
        throw new NotImplementedException();
    }

    public DataProperty getPartitionDataProperty(){
        return this.partitionDataProperty;
    }

    public Short getReplicationNum() {
        return this.replicationNum;
    }

    public boolean isInMemory() {
        return this.isInMemory;
    }

    public TTabletType getTabletType() {
        return this.tabletType;
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> tableProperties) throws AnalysisException {
        FeNameFormat.checkPartitionName(this.getPartitionName());

        // copy one. because ProperAnalyzer will remove entry after analyze
        Map<String, String> copiedTableProperties = Optional.ofNullable(tableProperties)
                .map(properties -> Maps.newHashMap(properties))
                .orElseGet(() -> new HashMap<>());
        Map<String, String> copiedPartitionProperties = Optional.ofNullable(this.getPartitionProperties())
                .map(properties -> Maps.newHashMap(properties))
                .orElseGet(() -> new HashMap<>());

        // The priority of the partition attribute is higher than that of the table
        Map<String, String> allProperties = new HashMap<>();
        copiedTableProperties.forEach((k, v) -> allProperties.put(k, v));
        copiedPartitionProperties.forEach((k, v) -> allProperties.put(k, v));

        // analyze data property
        this.partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(allProperties,
                DataProperty.DEFAULT_DATA_PROPERTY);

        // analyze replication num
        this.replicationNum = PropertyAnalyzer.analyzeReplicationNum(allProperties, FeConstants.default_replication_num);

        // analyze version info
        this.versionInfo = PropertyAnalyzer.analyzeVersionInfo(allProperties);

        // analyze in memory
        this.isInMemory = PropertyAnalyzer.analyzeBooleanProp(allProperties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);

        // analyze tabletType
        this.tabletType = PropertyAnalyzer.analyzeTabletType(allProperties);

        // check unknown properties
        if (copiedTableProperties.isEmpty() && !allProperties.isEmpty()) {
            Joiner.MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
            throw new AnalysisException("Unknown properties: " + mapJoiner.join(allProperties));
        }
    }


}