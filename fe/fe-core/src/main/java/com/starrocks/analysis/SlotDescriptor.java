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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SlotDescriptor.java

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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnStats;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TSlotDescriptor;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SlotDescriptor {

    private static final Logger LOG = LogManager.getLogger(SlotDescriptor.class);
    private final SlotId id;
    private final TupleDescriptor parent;
    private Type type;
    private Column column;  // underlying column, if there is one

    // for SlotRef.toSql() in the absence of a path
    private String label_;

    // Expr(s) materialized into this slot; multiple exprs for unions. Should be empty if
    // path_ is set.
    private List<Expr> sourceExprs_ = Lists.newArrayList();

    // if false, this slot doesn't need to be materialized in parent tuple
    // (and physical layout parameters are invalid)
    private boolean isMaterialized;

    // if false, this slot only used in scan node, if it's dict encoded,
    // just filter in dict code, there is no need to be decoded.
    private boolean isOutputColumn = true;

    // if false, this slot cannot be NULL
    private boolean isNullable;

    // physical layout parameters
    private int byteSize;
    private int byteOffset;  // within tuple
    private int nullIndicatorByte;  // index into byte array
    private int nullIndicatorBit; // index within byte
    private int slotIdx;          // index within tuple struct

    private ColumnStats stats;  // only set if 'column' isn't set
    // used for load to get more information of varchar and decimal
    // and for query result set metadata
    private Type originType;

    public SlotDescriptor(SlotId id, TupleDescriptor parent) {
        this.id = id;
        this.parent = parent;
        this.byteOffset = -1;  // invalid
        this.isMaterialized = false;
        this.isOutputColumn = false;
        this.isNullable = true;
    }

    public SlotDescriptor(SlotId id, String name, Type type, boolean isNullable) {
        this.id = id;
        this.label_ = name;
        this.type = type;
        this.isNullable = isNullable;
        this.parent = null;
    }

    public SlotDescriptor(SlotId id, TupleDescriptor parent, SlotDescriptor src) {
        this.id = id;
        this.parent = parent;
        this.byteOffset = src.byteOffset;
        this.nullIndicatorBit = src.nullIndicatorBit;
        this.nullIndicatorByte = src.nullIndicatorByte;
        this.slotIdx = src.slotIdx;
        this.isMaterialized = src.isMaterialized;
        this.isOutputColumn = src.isOutputColumn;
        this.column = src.column;
        this.isNullable = src.isNullable;
        this.byteSize = src.byteSize;
        this.stats = src.stats;
        this.type = src.type;
    }

    public void setMultiRef(boolean isMultiRef) {
    }

    public SlotId getId() {
        return id;
    }

    public TupleDescriptor getParent() {
        return parent;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        if (type.isInvalid()) {
            throw new SemanticException("slot type shouldn't be invalid");
        }
        this.type = type;
    }

    public Type getOriginType() {
        return originType;
    }

    public void setOriginType(Type type) {
        this.originType = type;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
        this.originType = column.getType();
        if (this.originType.isScalarType()) {
            ScalarType scalarType = (ScalarType) this.originType;
            if (this.originType.isDecimalV3()) {
                this.type = ScalarType.createDecimalV3Type(
                        scalarType.getPrimitiveType(),
                        scalarType.getScalarPrecision(),
                        scalarType.getScalarScale());
            } else {
                this.type = ScalarType.createType(this.originType.getPrimitiveType());
            }
        } else {
            this.type = this.originType.clone();
        }
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public void setIsMaterialized(boolean value) {
        isMaterialized = value;
    }

    public boolean isOutputColumn() {
        return isOutputColumn;
    }

    public void setIsOutputColumn(boolean value) {
        isOutputColumn = value;
    }

    public boolean getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(boolean value) {
        isNullable = value;
    }

    public void setStats(ColumnStats stats) {
        this.stats = stats;
    }

    public ColumnStats getStats() {
        if (stats == null) {
            if (column != null) {
                stats = column.getStats();
            } else {
                stats = new ColumnStats();
            }
        }
        return stats;
    }

    public String getLabel() {
        return label_;
    }

    public void setLabel(String label) {
        label_ = label;
    }

    public void setSourceExpr(Expr expr) {
        sourceExprs_ = Collections.singletonList(expr);
    }

    public List<Expr> getSourceExprs() {
        return sourceExprs_;
    }

    /**
     * Initializes a slot by setting its source expression information
     */
    public void initFromExpr(Expr expr) {
        setLabel(expr.toSql());
        Preconditions.checkState(sourceExprs_.isEmpty());
        setSourceExpr(expr);
        setStats(ColumnStats.fromExpr(expr));
        Preconditions.checkState(expr.getType().isValid());
        setType(expr.getType());
        // Vector query engine need the nullable info
        setIsNullable(expr.isNullable());
    }

    // TODO
    public TSlotDescriptor toThrift() {
        if (isNullable) {
            nullIndicatorBit = 1;
        } else {
            nullIndicatorBit = -1;
        }
        Preconditions.checkState(isMaterialized, "isMaterialized must be true");
        TSlotDescriptor tSlotDescriptor = new TSlotDescriptor();
        tSlotDescriptor.setId(id.asInt());
        tSlotDescriptor.setParent(parent.getId().asInt());
        if (originType != null) {
            tSlotDescriptor.setSlotType(originType.toThrift());
        } else {
            type = type.isNull() ? ScalarType.BOOLEAN : type;
            tSlotDescriptor.setSlotType(type.toThrift());
            if (column != null) {
                LOG.debug("column physical name:{}, column unique id:{}",
                        column.getPhysicalName(), column.getUniqueId());
                tSlotDescriptor.setCol_unique_id(column.getUniqueId());
            }
        }
        tSlotDescriptor.setColumnPos(-1);
        tSlotDescriptor.setByteOffset(-1);
        tSlotDescriptor.setNullIndicatorByte(-1);
        tSlotDescriptor.setNullIndicatorBit(nullIndicatorBit);
        tSlotDescriptor.setColName(((column != null) ? column.getPhysicalName() : ""));
        tSlotDescriptor.setSlotIdx(-1);
        tSlotDescriptor.setIsMaterialized(true);
        tSlotDescriptor.setIsOutputColumn(isOutputColumn);
        tSlotDescriptor.setIsNullable(isNullable);
        return tSlotDescriptor;
    }

    public String debugString() {
        String colStr = (column == null ? "null" : column.getName());
        String typeStr = (type == null ? "null" : type.toString());
        String parentTupleId = (parent == null) ? "null" : parent.getId().toString();
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("parent", parentTupleId)
                .add("col", colStr).add("type", typeStr).add("materialized", isMaterialized)
                .add("isOutputColumns", isOutputColumn)
                .add("byteSize", byteSize).add("byteOffset", byteOffset)
                .add("nullIndicatorByte", nullIndicatorByte)
                .add("nullIndicatorBit", nullIndicatorBit)
                .add("slotIdx", slotIdx).toString();
    }

    @Override
    public String toString() {
        return debugString();
    }
}
