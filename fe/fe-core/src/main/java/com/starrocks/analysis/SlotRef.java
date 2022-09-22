// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TSlotRef;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class SlotRef extends Expr {
    private TableName tblName;
    private String col;
    // Used in toSql
    private String label;

    // results of analysis
    protected SlotDescriptor desc;

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
    }

    public SlotRef(String label, SlotDescriptor desc) {
        this(desc);
        this.label = label;
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

    public void setTblName(TableName name) {
        this.tblName = name;
    }

    public void setDesc(SlotDescriptor desc) {
        this.desc = desc;
    }

<<<<<<< HEAD
=======
    public SlotDescriptor getSlotDescriptorWithoutCheck() {
        return desc;
    }

>>>>>>> f017b31f8 ([BugFix] Fix refresh mv with partition by not first column bug (#11533))
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
    public String toColumnLabel() {
        return label;
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
            msg.slot_ref = new TSlotRef(0,0);
        }

        msg.setOutput_column(outputColumn);
    }

    @Override
    public int hashCode() {
        if (desc != null) {
            return desc.getId().hashCode();
        }
        return Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase());
    }

    @Override
    public boolean equals(Object obj) {
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
        return true;
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

    @Override
    public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
        Preconditions.checkState(type.isValid());
        Preconditions.checkState(desc != null);
        if (slotIds != null) {
            slotIds.add(desc.getId());
        }
        if (tupleIds != null) {
            tupleIds.add(desc.getParent().getId());
        }
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
