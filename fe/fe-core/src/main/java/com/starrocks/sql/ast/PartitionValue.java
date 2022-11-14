// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

public class PartitionValue implements ParseNode {
    public static final PartitionValue MAX_VALUE = new PartitionValue();

    private String value;

    private PartitionValue() {

    }

    public PartitionValue(String value) {
        this.value = value;
    }

    public LiteralExpr getValue(Type type) throws AnalysisException {
        if (isMax()) {
            return LiteralExpr.createInfinity(type, true);
        } else {
            if (type == Type.DATETIME) {
                try {
                    return LiteralExpr.create(value, type);
                } catch (AnalysisException ex) {
                    // partition value allowed DATETIME type like DATE
                    LiteralExpr literalExpr = LiteralExpr.create(value, Type.DATE);
                    literalExpr.setType(Type.DATETIME);
                    return literalExpr;
                }
            } else {
                return LiteralExpr.create(value, type);
            }
        }
    }

    public boolean isMax() {
        return this == MAX_VALUE;
    }

    public String getStringValue() {
        if (isMax()) {
            return "MAXVALUE";
        } else {
            return value;
        }
    }
}
