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

package com.starrocks.sql.ast.expression;

import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TPlaceHolder;
import com.starrocks.thrift.TSlotRef;

/**
 * Convert {@link Expr} nodes into their normalized Thrift representation.
 */
public class ExprToNormalFormVisitor extends ExprToThriftVisitor {
    private final FragmentNormalizer normalizer;

    private ExprToNormalFormVisitor(FragmentNormalizer normalizer) {
        this.normalizer = normalizer;
    }

    public static TExpr treeToNormalForm(Expr expr, FragmentNormalizer normalizer) {
        TExpr result = new TExpr();
        ExprToNormalFormVisitor visitor = new ExprToNormalFormVisitor(normalizer);
        ExprToThriftVisitor.treeToThriftHelper(expr, result, visitor::visit);
        return result;
    }

    @Override
    public Void visitSlot(SlotRef node, TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        SlotDescriptor desc = node.getDesc();
        if (desc != null) {
            SlotId slotId = normalizer.isNotRemappingSlotId() ? desc.getId() : normalizer.remapSlotId(desc.getId());
            msg.slot_ref = new TSlotRef(slotId.asInt(), 0);
        } else {
            msg.slot_ref = new TSlotRef(0, 0);
        }
        msg.setOutput_column(node.getOutputColumn());
        return null;
    }

    @Override
    public Void visitPlaceHolderExpr(PlaceHolderExpr node, TExprNode msg) {
        msg.setNode_type(TExprNodeType.PLACEHOLDER_EXPR);
        msg.setVslot_ref(new TPlaceHolder());
        msg.vslot_ref.setNullable(node.isNullable());
        msg.vslot_ref.setSlot_id(normalizer.remapSlotId(new SlotId(node.getSlotId())).asInt());
        return null;
    }
}
