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

package com.starrocks.sql.optimizer.operator.physical;

import com.lancedb.lance.index.DistanceType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;

public class PhysicalLanceScanOperator extends PhysicalScanOperator {

    public PhysicalLanceScanOperator(LogicalLanceScanOperator scan) {
        super(OperatorType.PHYSICAL_LANCE_SCAN, scan);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalLanceScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalLanceScan(optExpression, context);
    }

    public boolean isVectorSearch() {
        return orderByElements != null && orderByElements.size() == 1;
    }

    public DistanceType getDistanceType() {
        CallOperator distanceFunction = (CallOperator) projection.getColumnRefMap()
                .get(orderByElements.get(0).getColumnRef());
        switch (distanceFunction.getFnName()) {
            case FunctionSet.COSINE_SIMILARITY:
                return DistanceType.Cosine;
            case FunctionSet.L2_DISTANCE:
                return DistanceType.L2;
            default:
                throw UnsupportedException.unsupportedException("unknown vector similarity function: "
                        + distanceFunction.getFunction());
        }
    }

    public String getVectorColumnName() {
        CallOperator distanceFunction = (CallOperator) projection.getColumnRefMap()
                .get(orderByElements.get(0).getColumnRef());
        ColumnRefOperator columnRefOperator = (ColumnRefOperator) distanceFunction.getArguments().get(0);
        return columnRefOperator.getName();
    }

    public float[] getSearchVector() {
        CallOperator distanceFunction = (CallOperator) projection.getColumnRefMap()
                .get(orderByElements.get(0).getColumnRef());
        CastOperator castOperator = (CastOperator) distanceFunction.getArguments().get(1);
        ArrayOperator arrayOperator = (ArrayOperator) castOperator.getArguments().get(0);
        ArrayType arrayType = (ArrayType) arrayOperator.getType();
        Type itemType = arrayType.getItemType();
        float[] res = new float[arrayOperator.getChildren().size()];
        for (int i = 0; i < arrayOperator.getChildren().size(); i++) {
            ConstantOperator child = (ConstantOperator) arrayOperator.getChild(i);
            if (itemType.isTinyint()) {
                res[i] = child.getTinyInt();
            } else if (itemType.isSmallint()) {
                res[i] = child.getSmallint();
            } else if (itemType.isInt()) {
                res[i] = child.getInt();
            } else if (itemType.isBigint()) {
                res[i] = child.getBigint();
            } else if (itemType.isDecimalOfAnyVersion()) {
                res[i] = child.getDecimal().floatValue();
            } else {
                throw UnsupportedException.unsupportedException("unknown vector literal type: " + itemType);
            }
        }
        return res;
    }
}
