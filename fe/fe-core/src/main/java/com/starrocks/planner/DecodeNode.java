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

import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.Pair;
import com.starrocks.thrift.TDecodeNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalDecodeNode;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DecodeNode extends PlanNode {
    // The dict id int column ids to dict string column ids
    private final Map<Integer, Integer> dictIdToStringIds;
    // The string functions have applied global dict optimization
    private final Map<SlotId, Expr> stringFunctions;

    // TupleId is changed when DecodeNode is interpolated, so pushing down runtime filters
    // across DecodeNode requires that replace the output SlotRef with the input SlotRef.
    private final Map<SlotRef, SlotRef> slotRefMap;

    public DecodeNode(PlanNodeId id,
                      TupleDescriptor tupleDescriptor,
                      PlanNode child,
                      Map<Integer, Integer> dictIdToStringIds,
                      Map<SlotId, Expr> stringFunctions,
                      Map<SlotRef, SlotRef> slotRefMap
                      ) {
        super(id, tupleDescriptor.getId().asList(), "Decode");
        addChild(child);
        this.dictIdToStringIds = dictIdToStringIds;
        this.stringFunctions = stringFunctions;
        this.slotRefMap = slotRefMap;
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DECODE_NODE;
        msg.decode_node = new TDecodeNode();
        msg.decode_node.setDict_id_to_string_ids(dictIdToStringIds);
        stringFunctions.forEach(
                (key, value) -> msg.decode_node.putToString_functions(key.asInt(), value.treeToThrift()));
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

    @Override
    public Optional<List<Expr>> candidatesOfSlotExpr(Expr expr, Function<Expr, Boolean> couldBound) {
        if (!(expr instanceof SlotRef)) {
            return Optional.empty();
        }
        if (!couldBound.apply(expr)) {
            return Optional.empty();
        }
        return Optional.ofNullable(slotRefMap.get(expr)).map(Lists::newArrayList);
    }

    @Override
    public boolean pushDownRuntimeFilters(DescriptorTable descTbl, RuntimeFilterDescription description,
                                          Expr probeExpr,
                                          List<Expr> partitionByExprs) {
        if (!canPushDownRuntimeFilter()) {
            return false;
        }

        if (!couldBound(probeExpr, description, descTbl)) {
            return false;
        }

        return pushdownRuntimeFilterForChildOrAccept(descTbl, description, probeExpr, candidatesOfSlotExpr(probeExpr, couldBound(description, descTbl)),
                partitionByExprs, candidatesOfSlotExprs(partitionByExprs, couldBoundForPartitionExpr()), 0, true);
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalDecodeNode decodeNode = new TNormalDecodeNode();
        List<Pair<SlotId, SlotId>> dictIdAndStrIdPairs = dictIdToStringIds.entrySet().stream().map(
                e -> Pair.create(normalizer.remapSlotId(new SlotId(e.getKey())), new SlotId(e.getValue()))
        ).sorted(Comparator.comparing(p -> p.first.asInt())).collect(Collectors.toList());
        List<Integer> fromDictIds = dictIdAndStrIdPairs.stream().map(p -> p.first.asInt())
                .collect(Collectors.toList());
        List<Integer> toStringIds = dictIdAndStrIdPairs.stream().map(p -> normalizer.remapSlotId(p.second).asInt())
                .collect(Collectors.toList());
        decodeNode.setFrom_dict_ids(fromDictIds);
        decodeNode.setTo_string_ids(toStringIds);
        normalizer.addSlotsUseAggColumns(stringFunctions);
        Pair<List<Integer>, List<ByteBuffer>> slotIdsAndExprs = normalizer.normalizeSlotIdsAndExprs(stringFunctions);
        decodeNode.setSlot_ids(slotIdsAndExprs.first);
        decodeNode.setString_functions(slotIdsAndExprs.second);
        planNode.setNode_type(TPlanNodeType.DECODE_NODE);
        planNode.setDecode_node(decodeNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }
}
