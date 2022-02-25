// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/PartitionValue.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

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
