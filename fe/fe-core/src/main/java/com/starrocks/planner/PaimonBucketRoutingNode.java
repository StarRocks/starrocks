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

import com.starrocks.planner.expression.ExprToThrift;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPaimonBucketRouting;
import com.starrocks.thrift.TPaimonTableSink;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Exec-only native calculation immediately before the final fixed-bucket writer exchange. */
public final class PaimonBucketRoutingNode extends ProjectNode {
    public static final int ROUTING_PROTOCOL_VERSION = 1;
    public static final int MAX_SINK_PAYLOAD_BYTES = 1024 * 1024;

    private final TPaimonBucketRouting routing;
    private final SlotId bucketSlot;

    public PaimonBucketRoutingNode(PlanNodeId id, TupleDescriptor tuple, PlanNode child,
                                  Map<SlotId, Expr> projection, List<Expr> schemaExprs,
                                  SlotId bucketSlot, TPaimonTableSink sink) {
        super(id, tuple, child, projection, Collections.emptyMap());
        this.bucketSlot = bucketSlot;
        planNodeName = "PAIMON BUCKET ROUTING";
        TPaimonTableSink target = sink.deepCopy();
        target.setRouting_bucket_slot_id(bucketSlot.asInt());
        target.setBucket_routed(false);
        byte[] payload;
        try {
            payload = new TSerializer(new TBinaryProtocol.Factory()).serialize(target);
        } catch (TException e) {
            // The descriptor contains cloud credentials. Do not include it or serializer details in errors.
            throw new SemanticException("Cannot encode Paimon bucket routing write contract");
        }
        if (payload.length == 0 || payload.length > MAX_SINK_PAYLOAD_BYTES) {
            throw new SemanticException("Paimon bucket routing write contract exceeds the 1 MiB limit");
        }
        routing = new TPaimonBucketRouting();
        routing.setProtocol_version(ROUTING_PROTOCOL_VERSION);
        routing.setSink_payload(ByteBuffer.wrap(payload));
        routing.setBucket_slot_id(bucketSlot.asInt());
        routing.setSchema_exprs(ExprToThrift.treesToThrift(schemaExprs));
    }

    @Override
    protected void toThrift(TPlanNode node) {
        super.toThrift(node);
        // A new explicit node type makes an older BE fail closed rather than ignoring the route descriptor.
        node.setNode_type(TPlanNodeType.PAIMON_BUCKET_ROUTING_NODE);
        node.project_node.setPaimon_bucket_routing(routing.deepCopy());
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return super.getNodeExplainString(prefix, detailLevel)
                + prefix + "native bucket slot: " + bucketSlot + "\n";
    }

    @Override
    public List<SlotId> getOutputSlotIds(DescriptorTable descriptorTable) {
        List<SlotId> output = new ArrayList<>(super.getOutputSlotIds(descriptorTable));
        output.add(bucketSlot);
        return output;
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }

    @Override
    public boolean isTrivial() {
        return false;
    }
}
