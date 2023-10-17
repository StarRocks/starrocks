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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/MVColumnItem.java

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

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;

import java.util.Set;

/**
 * This is a result of semantic analysis for AddMaterializedViewClause.
 * It is used to construct real mv column in MaterializedViewHandler.
 * It does not include all of column properties.
 * It just a intermediate variable between semantic analysis and final handler.
 */
public class MVColumnItem {
    private final String name;
    // the origin type of slot ref
    private Type type;
    private boolean isKey;
    private AggregateType aggregationType;
    private boolean isAllowNull;
    private boolean isAggregationTypeImplicit;
    private Expr defineExpr;
    private Set<String> baseColumnNames;

    public MVColumnItem(String name, Type type, AggregateType aggregateType, boolean isAggregationTypeImplicit,
                        Expr defineExpr, boolean isAllowNull, Set<String> baseColumnNames) {
        this.name = name;
        this.type = type;
        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
        this.defineExpr = defineExpr;
        this.isAllowNull = isAllowNull;
        this.baseColumnNames = baseColumnNames;
    }

    public boolean isAllowNull() {
        return isAllowNull;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setIsKey(boolean key) {
        isKey = key;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setAggregationType(AggregateType aggregationType, boolean isAggregationTypeImplicit) {
        this.aggregationType = aggregationType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public AggregateType getAggregationType() {
        return aggregationType;
    }

    public boolean isAggregationTypeImplicit() {
        return isAggregationTypeImplicit;
    }

    public Expr getDefineExpr() {
        return defineExpr;
    }

    public void setDefineExpr(Expr defineExpr) {
        this.defineExpr = defineExpr;
    }

    public Set<String> getBaseColumnNames() {
        return baseColumnNames;
    }

    public Column toMVColumn(OlapTable olapTable) throws DdlException {
        Column baseColumn = olapTable.getBaseColumn(name);
        Column result;
        boolean useLightSchemaChange = olapTable.getUseLightSchemaChange();
        if (baseColumn == null) {
            result = new Column(name, type, isKey, aggregationType, isAllowNull,
                    null, "");
            if (defineExpr != null) {
                result.setDefineExpr(defineExpr);
            }
            if (useLightSchemaChange) {
                int nextUniqueId = olapTable.incAndGetMaxColUniqueId();
                result.setUniqueId(nextUniqueId);
            }
        } else {
            result = new Column(baseColumn);
        }
        result.setName(name);
        result.setIsKey(isKey);
        result.setAggregationType(aggregationType, isAggregationTypeImplicit);
        return result;
    }
}
