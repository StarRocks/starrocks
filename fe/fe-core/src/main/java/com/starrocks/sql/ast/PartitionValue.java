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


package com.starrocks.sql.ast;

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.parser.NodePosition;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class PartitionValue implements ParseNode {
    public static final PartitionValue MAX_VALUE = new PartitionValue();

    private final NodePosition pos;

    private String value;

    private PartitionValue() {
        this.pos = NodePosition.ZERO;
    }

    public PartitionValue(String value) {
        this(value, NodePosition.ZERO);
    }

    public PartitionValue(String value, NodePosition pos) {
        this.pos = pos;
        this.value = value;
    }

    public static PartitionValue ofDateTime(LocalDateTime dateTime) {
        return new PartitionValue(dateTime.format(DateUtils.DATE_FORMATTER_UNIX));
    }

    public static PartitionValue ofDate(LocalDate date) {
        return ofDateTime(LocalDateTime.of(date, LocalTime.MIN));
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

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
