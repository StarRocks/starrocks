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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SlotRef.java

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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.planner.FragmentNormalizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TSlotRef;
import org.apache.arrow.util.VisibleForTesting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class SlotRef extends Expr {
    private TableName tblName;
    private String col;
    // Used in toSql
    private String label;

    private QualifiedName qualifiedName;

    // results of analysis
    protected SlotDescriptor desc;

    // Only Struct Type need this field
    // Record access struct subfield path position
    // Example: struct type: col: STRUCT<c1: INT, c2: STRUCT<c1: INT, c2: DOUBLE>>,
    // We execute sql: `SELECT col FROM table`, the usedStructField value is [].
    // We execute sql: `SELECT col.c2 FROM table`, the usedStructFieldPos value is [1].
    // We execute sql: `SELECT col.c2.c1 FROM table`, the usedStructFieldPos value is [1, 0].
    private ImmutableList<Integer> usedStructFieldPos;

    // now it is used in Analyzer phase of creating mv to decide the field nullable of mv
    // can not use desc because the slotId is unknown in Analyzer phase
    private boolean nullable = true;

    // Only used write
    private SlotRef() {
        super();
    }

    public SlotRef(TableName tblName, String col) {
        super();
        this.tblName = tblName;
        this.col = col;
        this.label = "`" + col + "`";
    }

    public SlotRef(TableName tblName, String col, String label) {
        super();
        this.tblName = tblName;
        this.col = col;
        this.label = label;
    }

    public SlotRef(QualifiedName qualifiedName) {
        List<String> parts = qualifiedName.getParts();
        // If parts.size() = 1, it must be a column name. Like `Select a FROM table`.
        // If parts.size() = [2, 3, 4], it maybe a column name or specific struct subfield name.
        Preconditions.checkArgument(parts.size() > 0);
        this.qualifiedName = QualifiedName.of(qualifiedName.getParts());
        if (parts.size() == 1) {
            this.col = parts.get(0);
            this.label = parts.get(0);
        } else if (parts.size() == 2) {
            this.tblName = new TableName(null, parts.get(0));
            this.col = parts.get(1);
            this.label = parts.get(1);
        } else if (parts.size() == 3) {
            this.tblName = new TableName(parts.get(0), parts.get(1));
            this.col = parts.get(2);
            this.label = parts.get(2);
        } else if (parts.size() == 4) {
            this.tblName = new TableName(parts.get(0), parts.get(1), parts.get(2));
            this.col = parts.get(3);
            this.label = parts.get(3);
        } else {
            // If parts.size() > 4, it must refer to a struct subfield name, so we set SlotRef's TableName null value,
            // set col, label a qualified name here[Of course it's a wrong value].
            // Correct value will be parsed in Analyzer according context.
            this.tblName = null;
            this.col = qualifiedName.toString();
            this.label = qualifiedName.toString();
        }
    }

    // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
    // a table's column.
    public SlotRef(SlotDescriptor desc) {
        super();
        this.tblName = null;
        this.col = desc.getLabel();
        this.desc = desc;
        this.type = desc.getType();
        this.originType = desc.getOriginType();
        this.label = null;
        if (this.type.isChar()) {
            this.type = Type.VARCHAR;
        }
        analysisDone();
    }

    protected SlotRef(SlotRef other) {
        super(other);
        tblName = other.tblName;
        col = other.col;
        label = other.label;
        desc = other.desc;
        qualifiedName = other.qualifiedName;
        usedStructFieldPos = other.usedStructFieldPos;
    }

    public SlotRef(String label, SlotDescriptor desc) {
        this(desc);
        this.label = label;
    }

    public SlotRef(SlotId slotId) {
        this(new SlotDescriptor(slotId, "", Type.INVALID, false));
    }

    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public void setUsedStructFieldPos(List<Integer> usedStructFieldPos) {
        this.usedStructFieldPos = ImmutableList.copyOf(usedStructFieldPos);
    }

    public List<Integer> getUsedStructFieldPos() {
        return usedStructFieldPos;
    }

    // When SlotRef is accessing struct subfield, we need to reset SlotRef's type and col name
    // Do this is for compatible with origin SlotRef
    public void resetStructInfo() {
        Preconditions.checkArgument(type.isStructType());
        Preconditions.checkArgument(usedStructFieldPos.size() > 0);

        StringBuilder colStr = new StringBuilder();
        colStr.append(col);

        setOriginType(type);
        Type tmpType = type;
        for (int pos : usedStructFieldPos) {
            StructField structField = ((StructType) tmpType).getField(pos);
            colStr.append(".");
            colStr.append(structField.getName());
            tmpType = structField.getType();
        }
        // Set type to subfield's type
        type = tmpType;
        // col name like a.b.c
        col = colStr.toString();
    }

    @Override
    public Expr clone() {
        return new SlotRef(this);
    }

    public SlotDescriptor getDesc() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc;
    }

    public SlotId getSlotId() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }

    public Column getColumn() {
        if (desc == null) {
            return null;
        } else {
            return desc.getColumn();
        }
    }

    public boolean isFromLambda() {
        return tblName != null && tblName.getTbl().equals(TableName.LAMBDA_FUNC_TABLE);
    }

    public void setTblName(TableName name) {
        this.tblName = name;
    }

    public void setDesc(SlotDescriptor desc) {
        this.desc = desc;
    }

    public void setType(Type type) {
        super.setType(type);
        if (desc != null) {
            desc.setType(type);
        }
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public SlotDescriptor getSlotDescriptorWithoutCheck() {
        return desc;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    public String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("slotDesc", desc != null ? desc.debugString() : "null");
        helper.add("col", col);
        helper.add("label", label);
        helper.add("tblName", tblName != null ? tblName.toSql() : "null");
        return helper.toString();
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        if (tblName != null) {
            return tblName.toSql() + "." + "`" + col + "`";
        } else if (label != null) {
            return label + sb.toString();
        } else if (desc.getSourceExprs() != null) {
            sb.append("<slot ").append(desc.getId().asInt()).append(">");
            for (Expr expr : desc.getSourceExprs()) {
                sb.append(" ");
                sb.append(expr.toSql());
            }
            return sb.toString();
        } else {
            return "<slot " + desc.getId().asInt() + ">" + sb.toString();
        }
    }

    @Override
    public String explainImpl() {
        if (label != null) {
            return "[" + label + "," +
                    " " + desc.getType() + "," +
                    " " + desc.getIsNullable() + "]";
        } else {
            return "[" + desc.getId().asInt() + "," +
                    " " + desc.getType() + "," +
                    " " + desc.getIsNullable() + "]";
        }
    }

    @Override
    public String toMySql() {
        if (col != null) {
            return col;
        } else {
            return "<slot " + Integer.toString(desc.getId().asInt()) + ">";
        }
    }

    @Override
    public String toJDBCSQL(boolean isMySQL) {
        if (col != null) {
            return isMySQL ? "`" + col + "`" : col;
        } else {
            return "<slot " + Integer.toString(desc.getId().asInt()) + ">";
        }
    }

    public TableName getTableName() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        if (tblName == null) {
            Preconditions.checkNotNull(desc.getParent());
            if (desc.getParent().getRef() == null) {
                return null;
            }
            return desc.getParent().getRef().getName();
        }
        return tblName;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        if (desc != null) {
            if (desc.getParent() != null) {
                msg.slot_ref = new TSlotRef(desc.getId().asInt(), desc.getParent().getId().asInt());
            } else {
                // tuple id is meaningless here
                msg.slot_ref = new TSlotRef(desc.getId().asInt(), 0);
            }
        } else {
            // slot id and tuple id are meaningless here
            msg.slot_ref = new TSlotRef(0, 0);
        }

        msg.setOutput_column(outputColumn);
    }

    @Override
    public void toNormalForm(TExprNode msg, FragmentNormalizer normalizer) {
        msg.node_type = TExprNodeType.SLOT_REF;
        if (desc != null) {
            SlotId slotId = normalizer.isNotRemappingSlotId() ? desc.getId() : normalizer.remapSlotId(desc.getId());
            // tuple id is meaningless here
            msg.slot_ref = new TSlotRef(slotId.asInt(), 0);
        } else {
            // slot id and tuple id are meaningless here
            msg.slot_ref = new TSlotRef(0, 0);
        }
        msg.setOutput_column(outputColumn);
    }

    @Override
    public int hashCode() {
        if (desc != null) {
            return desc.getId().hashCode();
        }
        if (usedStructFieldPos != null) {
            // Means this SlotRef is going to access subfield in StructType
            return Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase(), usedStructFieldPos);
        } else {
            return Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase());
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        SlotRef other = (SlotRef) obj;
        // check slot ids first; if they're both set we only need to compare those
        // (regardless of how the ref was constructed)
        if (desc != null && other.desc != null) {
            return desc.getId().equals(other.desc.getId());
        }
        if ((tblName == null) != (other.tblName == null)) {
            return false;
        }
        if (tblName != null && !tblName.equals(other.tblName)) {
            return false;
        }
        if ((col == null) != (other.col == null)) {
            return false;
        }
        if (col != null && !col.equalsIgnoreCase(other.col)) {
            return false;
        }

        if (usedStructFieldPos != null && !usedStructFieldPos.equals(other.usedStructFieldPos)) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    public boolean isNullable() {
        if (desc != null) {
            return desc.getIsNullable();
        }
        return nullable;
    }

    @Override
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        Preconditions.checkState(desc != null);
        for (TupleId tid : tids) {
            if (tid.equals(desc.getParent().getId())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBound(SlotId slotId) {
        Preconditions.checkState(isAnalyzed);
        return desc.getId().equals(slotId);
    }

    public Table getTable() {
        Preconditions.checkState(desc != null);
        Table table = desc.getParent().getTable();
        return table;
    }

    public String getColumnName() {
        return col;
    }

    public void setColumnName(String columnName) {
        this.col = columnName;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TableName
        if (tblName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            tblName.write(out);
        }
        Text.writeString(out, col);
    }

    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            tblName = new TableName();
            tblName.readFields(in);
        }
        col = Text.readString(in);
    }

    public static SlotRef read(DataInput in) throws IOException {
        SlotRef slotRef = new SlotRef();
        slotRef.readFields(in);
        return slotRef;
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) throws SemanticException {
        return visitor.visitSlot(this, context);
    }

    public TableName getTblNameWithoutAnalyzed() {
        return tblName;
    }

    @Override
    public boolean isSelfMonotonic() {
        return true;
    }
}
