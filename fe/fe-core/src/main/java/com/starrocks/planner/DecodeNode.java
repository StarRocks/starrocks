// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.planner;

import com.starrocks.analysis.TupleId;
import com.starrocks.thrift.TDecodeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.Map;

public class DecodeNode extends PlanNode{
    private final Map<Integer, Integer> dictIdToStringIds;

    public DecodeNode(PlanNodeId id,
                      ArrayList<TupleId> tupleIds,
                      PlanNode child,
                      Map<Integer, Integer> dictIdToStringIds) {
        super(id, tupleIds, "Decode");
        addChild(child);
        this.tblRefIds = child.tblRefIds;
        this.dictIdToStringIds = dictIdToStringIds;
    }

    @Override
    public boolean isVectorized() {
        return true;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DECODE_NODE;
        msg.decode_node = new TDecodeNode();
        msg.decode_node.setDict_id_to_string_ids(dictIdToStringIds);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        for (Map.Entry<Integer, Integer> kv : dictIdToStringIds.entrySet()) {
            output.append(prefix);
            output.append("<dict id ").
                    append(kv.getKey()).
                    append("> : ").
                    append("<string id ").append(kv.getValue()).append(">").
                    append("\n");
        }
        return output.toString();
    }
}