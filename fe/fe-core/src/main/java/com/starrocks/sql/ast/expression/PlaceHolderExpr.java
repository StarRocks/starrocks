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

import com.starrocks.catalog.Type;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.planner.SlotId;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TPlaceHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// placeholder is mainly used for function calls.
// Unlike a slotRef, it does not represent a real column, but is only used as an input column for function calls.
// now it was only used in global dictionary optimization, and express lambda inputs.
public class PlaceHolderExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(PlaceHolderExpr.class);

    private final int slotId;
    boolean nullable;

    public PlaceHolderExpr(int slotId, boolean nullable, Type type) {
        super();
        this.slotId = slotId;
        this.nullable = nullable;
        this.type = type;
    }

    public int getSlotId() {
        return slotId;
    }


    @Override
    public void toNormalForm(TExprNode msg, FragmentNormalizer normalizer) {
        msg.setNode_type(TExprNodeType.PLACEHOLDER_EXPR);
        msg.setVslot_ref(new TPlaceHolder());
        msg.vslot_ref.setNullable(nullable);
        msg.vslot_ref.setSlot_id(normalizer.remapSlotId(new SlotId(slotId)).asInt());
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public Expr clone() {
        return new PlaceHolderExpr(slotId, nullable, type);
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitPlaceHolderExpr(this, context);
    }
}
