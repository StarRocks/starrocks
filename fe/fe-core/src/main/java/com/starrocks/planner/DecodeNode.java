// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.planner;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.thrift.TDecodeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.Map;

public class DecodeNode extends PlanNode{
    // The dict id int column ids to dict string column ids
    private final Map<Integer, Integer> dictIdToStringIds;
    // The string functions have applied global dict optimization
    private final Map<SlotId, Expr> stringFunctions;

    public DecodeNode(PlanNodeId id,
                      TupleDescriptor tupleDescriptor,
                      PlanNode child,
                      Map<Integer, Integer> dictIdToStringIds,
                      Map<SlotId, Expr> stringFunctions) {
        super(id, tupleDescriptor.getId().asList(), "Decode");
        addChild(child);
        this.dictIdToStringIds = dictIdToStringIds;
        this.stringFunctions = stringFunctions;
    }

    @Override
    public boolean isVectorized() {
        return true;
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DECODE_NODE;
        msg.decode_node = new TDecodeNode();
        msg.decode_node.setDict_id_to_string_ids(dictIdToStringIds);
        stringFunctions.forEach((key, value) -> msg.decode_node.putToString_functions(key.asInt(), value.treeToThrift()));
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
        if (!stringFunctions.isEmpty()) {
            output.append(prefix);
            output.append("string functions:\n");
            for (Map.Entry<SlotId, Expr> kv : stringFunctions.entrySet()) {
                output.append(prefix);
                output.append("<function id ").
                        append(kv.getKey()).
                        append("> : ").
                        append(kv.getValue().toSql()).
                        append("\n");
            }
        }
        return output.toString();
    }
}
