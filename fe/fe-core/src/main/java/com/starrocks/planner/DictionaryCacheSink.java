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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TDictionaryCacheSink;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TOlapTableColumnParam;
import com.starrocks.thrift.TOlapTableIndexSchema;
import com.starrocks.thrift.TOlapTableSchemaParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DictionaryCacheSink extends DataSink {
    private final List<TNetworkAddress> nodes;
    private final Dictionary dictionary;
    private final long txnId;

    public DictionaryCacheSink(List<TNetworkAddress> nodes, Dictionary dictionary, long txnId) {
        this.nodes = nodes;
        this.dictionary = dictionary;
        this.txnId = txnId;
    }

    public Dictionary getDictionary() {
        return this.dictionary;
    }

    public List<TNetworkAddress> getNodes() {
        return nodes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return ""; /* No need */
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink result = new TDataSink(TDataSinkType.DICTIONARY_CACHE_SINK);
        TDictionaryCacheSink tTDictionaryCacheSink = new TDictionaryCacheSink();
        tTDictionaryCacheSink.setNodes(nodes);
        tTDictionaryCacheSink.setDictionary_id(dictionary.getDictionaryId());
        tTDictionaryCacheSink.setTxn_id(txnId);
        tTDictionaryCacheSink.setSchema(buildTSchema());
        tTDictionaryCacheSink.setMemory_limit(dictionary.getMemoryLimit());
        tTDictionaryCacheSink.setKey_size(dictionary.getKeys().size());
        result.setDictionary_cache_sink(tTDictionaryCacheSink);
        return result;
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

    private TOlapTableSchemaParam createSchema(long dbId, Table table, TupleDescriptor tupleDescriptor) {
        if (table instanceof OlapTable) {
            return OlapTableSink.createSchema(dbId, (OlapTable) table, tupleDescriptor);
        }

        // For the non-olaptable queryable object, construct the fake TOlapTableSchemaParam manually
        TOlapTableSchemaParam schemaParam = new TOlapTableSchemaParam();
        schemaParam.setDb_id(dbId);
        schemaParam.setTable_id(table.getId());
        schemaParam.setVersion(1);

        schemaParam.tuple_desc = tupleDescriptor.toThrift();
        for (SlotDescriptor slotDesc : tupleDescriptor.getSlots()) {
            schemaParam.addToSlot_descs(slotDesc.toThrift());
        }

        List<String> columns = Lists.newArrayList();
        List<TColumn> columnsDesc = Lists.newArrayList();
        List<Integer> columnSortKeyUids = Lists.newArrayList();
        columns.addAll(table.getBaseSchema().stream().map(column -> column.getColumnId().getId())
                .collect(Collectors.toList()));
        for (Column column : table.getBaseSchema()) {
            TColumn tColumn = column.toThrift();
            tColumn.setColumn_name(column.getColumnId().getId());
            columnsDesc.add(tColumn);
        }
        TOlapTableColumnParam columnParam = new TOlapTableColumnParam(columnsDesc, columnSortKeyUids, 0);
        TOlapTableIndexSchema indexSchema = new TOlapTableIndexSchema(0, columns, 0);
        indexSchema.setColumn_param(columnParam);
        schemaParam.addToIndexes(indexSchema);
        return schemaParam;
    }

    private TOlapTableSchemaParam buildTSchema() {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(new ConnectContext(),
                                        dictionary.getCatalogName(), dictionary.getDbName());
        String queryableObject = dictionary.getQueryableObject();
        Table tbl = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(new ConnectContext(),
                                        dictionary.getCatalogName(), dictionary.getDbName(), queryableObject);
        Preconditions.checkNotNull(tbl);

        TupleDescriptor tupleDescriptor = new TupleDescriptor(TupleId.createGenerator().getNextId());
        int currentSlotId = 1;
        Map<String, SlotDescriptor> keySlots = new HashMap();
        Map<String, SlotDescriptor> valueSlots = new HashMap();

        for (Column column : tbl.getBaseSchema()) {
            if (dictionary.getKeys().contains(column.getName())) {
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(currentSlotId++), tupleDescriptor);
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsMaterialized(true);

                keySlots.put(column.getName(), slotDescriptor);
            } else if (dictionary.getValues().contains(column.getName())) {
                SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(currentSlotId++), tupleDescriptor);
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsMaterialized(true);

                valueSlots.put(column.getName(), slotDescriptor);
            }
        }

        // key order by definition
        for (String key : dictionary.getKeys()) {
            SlotDescriptor slotDescriptor = keySlots.get(key);
            Preconditions.checkNotNull(slotDescriptor);
            tupleDescriptor.addSlot(slotDescriptor);
        }
        // value order by definition
        for (String value : dictionary.getValues()) {
            SlotDescriptor slotDescriptor = valueSlots.get(value);
            Preconditions.checkNotNull(slotDescriptor);
            tupleDescriptor.addSlot(slotDescriptor);
        }

        return createSchema(db.getId(), tbl, tupleDescriptor);
    }

}
