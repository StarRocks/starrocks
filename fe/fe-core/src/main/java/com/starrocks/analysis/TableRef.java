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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/TableRef.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Superclass of all table references, including references to views, base tables
 * (Hdfs, HBase or DataSource tables), and nested collections. Contains the join
 * specification. An instance of a TableRef (and not a subclass thereof) represents
 * an unresolved table reference that must be resolved during analysis. All resolved
 * table references are subclasses of TableRef.
 * <p>
 * The analysis of table refs follows a two-step process:
 * <p>
 * 1. Resolution: A table ref's path is resolved and then the generic TableRef is
 * replaced by a concrete table ref (a BaseTableRef, CollectionTabeRef or ViewRef)
 * in the originating stmt and that is given the resolved path. This step is driven by
 * Analyzer.resolveTableRef().
 * <p>
 * 2. Analysis/registration: After resolution, the concrete table ref is analyzed
 * to register a tuple descriptor for its resolved path and register other table-ref
 * specific state with the analyzer (e.g., whether it is outer/semi joined, etc.).
 * <p>
 * Therefore, subclasses of TableRef should never call the analyze() of its superclass.
 * <p>
 * TODO for 2.3: The current TableRef class hierarchy and the related two-phase analysis
 * feels convoluted and is hard to follow. We should reorganize the TableRef class
 * structure for clarity of analysis and avoid a table ref 'switching genders' in between
 * resolution and registration.
 * <p>
 * TODO for 2.3: Rename this class to CollectionRef and re-consider the naming and
 * structure of all subclasses.
 */
public class TableRef implements ParseNode, Writable {
    private static final Logger LOG = LogManager.getLogger(TableRef.class);
    @SerializedName(value = "name")
    protected TableName name;
    @SerializedName(value = "partitionNames")
    private PartitionNames partitionNames = null;
    private List<Long> tabletIds = Lists.newArrayList();

    // Legal aliases of this table ref. Contains the explicit alias as its sole element if
    // there is one. Otherwise, contains the two implicit aliases. Implicit aliases are set
    // in the c'tor of the corresponding resolved table ref (subclasses of TableRef) during
    // analysis. By convention, for table refs with multiple implicit aliases, aliases_[0]
    // contains the fully-qualified implicit alias to ensure that aliases_[0] always
    // uniquely identifies this table ref regardless of whether it has an explicit alias.
    @SerializedName(value = "aliases")
    protected String[] aliases_;

    // Indicates whether this table ref is given an explicit alias,
    protected boolean hasExplicitAlias_;

    protected JoinOperator joinOp;
    protected List<String> usingColNames;
    private boolean isLateral;
    private ArrayList<String> joinHints;
    private ArrayList<String> sortHints;
    private ArrayList<String> commonHints; //The Hints is set by user
    private boolean isForcePreAggOpened;
    // Mark left anti join is rewritten from not in or not exists
    private boolean isJoinRewrittenFromNotIn;
    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    protected Expr onClause;

    // the ref to the left of us, if we're part of a JOIN clause
    protected TableRef leftTblRef;

    // true if this TableRef has been analyzed; implementing subclass should set it to true
    // at the end of analyze() call.
    protected boolean isAnalyzed;

    // Lists of table ref ids and materialized tuple ids of the full sequence of table
    // refs up to and including this one. These ids are cached during analysis because
    // we may alter the chain of table refs during plan generation, but we still rely
    // on the original list of ids for correct predicate assignment.
    // Populated in analyzeJoin().
    protected List<TupleId> allTableRefIds_ = Lists.newArrayList();
    protected List<TupleId> allMaterializedTupleIds_ = Lists.newArrayList();

    // All physical tuple ids that this table ref is correlated with:
    // Tuple ids of root descriptors from outer query blocks that this table ref
    // (if a CollectionTableRef) or contained CollectionTableRefs (if an InlineViewRef)
    // are rooted at. Populated during analysis.
    protected List<TupleId> correlatedTupleIds_ = Lists.newArrayList();

    // analysis output
    protected TupleDescriptor desc;

    protected final NodePosition pos;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    public TableRef() {
        // for persist
        pos = NodePosition.ZERO;
    }

    public TableRef(TableName name, String alias) {
        this(name, alias, null, NodePosition.ZERO);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames) {
        this(name, alias, partitionNames, null, null, NodePosition.ZERO);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames, NodePosition pos) {
        this(name, alias, partitionNames, null, null, pos);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames, ArrayList<Long> tableIds,
                    ArrayList<String> commonHints, NodePosition pos) {
        this.pos = pos;
        this.name = name;
        if (alias != null) {
            aliases_ = new String[] {alias};
            hasExplicitAlias_ = true;
        } else {
            hasExplicitAlias_ = false;
        }
        this.partitionNames = partitionNames;
        if (tableIds != null) {
            this.tabletIds = tableIds;
        }
        this.commonHints = commonHints;
        isAnalyzed = false;
    }

    // Only used to clone
    // this will reset all the 'analyzed' stuff
    protected TableRef(TableRef other) {
        pos = other.pos;
        name = other.name;
        aliases_ = other.aliases_;
        hasExplicitAlias_ = other.hasExplicitAlias_;
        joinOp = other.joinOp;
        // NOTE: joinHints and sortHints maybe changed after clone. so we new one List.
        joinHints =
                (other.joinHints != null) ? Lists.newArrayList(other.joinHints) : null;
        sortHints =
                (other.sortHints != null) ? Lists.newArrayList(other.sortHints) : null;
        onClause = (other.onClause != null) ? other.onClause.clone().reset() : null;
        partitionNames = (other.partitionNames != null) ? new PartitionNames(other.partitionNames) : null;
        commonHints = other.commonHints;

        usingColNames =
                (other.usingColNames != null) ? Lists.newArrayList(other.usingColNames) : null;
        // The table ref links are created at the statement level, so cloning a set of linked
        // table refs is the responsibility of the statement.
        leftTblRef = null;
        isAnalyzed = other.isAnalyzed;
        allTableRefIds_ = Lists.newArrayList(other.allTableRefIds_);
        allMaterializedTupleIds_ = Lists.newArrayList(other.allMaterializedTupleIds_);
        correlatedTupleIds_ = Lists.newArrayList(other.correlatedTupleIds_);
        desc = other.desc;
        isJoinRewrittenFromNotIn = other.isJoinRewrittenFromNotIn;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNRESOLVED_TABLE_REF, tableRefToSql());
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    /**
     * Creates and returns a empty TupleDescriptor registered with the analyzer. The
     * returned tuple descriptor must have its source table set via descTbl.setTable()).
     * This method is called from the analyzer when registering this table reference.
     */
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNRESOLVED_TABLE_REF, tableRefToSql());
        return null;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public JoinOperator getJoinOp() {
        // if it's not explicitly set, we're doing an inner join
        return (joinOp == null ? JoinOperator.INNER_JOIN : joinOp);
    }

    public void setOnClause(Expr e) {
        this.onClause = e;
    }

    public TableName getName() {
        return name;
    }

    public void setName(TableName name) {
        this.name = name;
    }

    public void setSortHints(ArrayList<String> hints) {
        this.sortHints = hints;
    }

    public boolean isLateral() {
        return isLateral;
    }

    public void setLateral(boolean lateral) {
        isLateral = lateral;
    }

    private String joinOpToSql() {
        Preconditions.checkState(joinOp != null);
        switch (joinOp) {
            case INNER_JOIN:
                return "INNER JOIN";
            case LEFT_OUTER_JOIN:
                return "LEFT OUTER JOIN";
            case LEFT_SEMI_JOIN:
                return "LEFT SEMI JOIN";
            case LEFT_ANTI_JOIN:
                return "LEFT ANTI JOIN";
            case RIGHT_SEMI_JOIN:
                return "RIGHT SEMI JOIN";
            case RIGHT_ANTI_JOIN:
                return "RIGHT ANTI JOIN";
            case RIGHT_OUTER_JOIN:
                return "RIGHT OUTER JOIN";
            case FULL_OUTER_JOIN:
                return "FULL OUTER JOIN";
            case CROSS_JOIN:
                return "CROSS JOIN";
            default:
                return "bad join op: " + joinOp.toString();
        }
    }

    /**
     * Return the table ref presentation to be used in the toSql string
     */
    public String tableRefToSql() {
        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) {
            aliasSql = ToSqlUtils.getIdentSql(alias);
        }
        return name.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
    }

    public String getAlias() {
        if (!hasExplicitAlias()) {
            return name.toString();
        }
        return getUniqueAlias();
    }

    /**
     * Returns all legal aliases of this table ref.
     */
    public String[] getAliases() {
        return aliases_;
    }

    /**
     * Returns the explicit alias or the fully-qualified implicit alias. The returned alias
     * is guaranteed to be unique (i.e., column/field references against the alias cannot
     * be ambiguous).
     */
    public String getUniqueAlias() {
        return aliases_[0];
    }

    /**
     * Returns true if this table ref has an explicit alias.
     * Note that getAliases().length() == 1 does not imply an explicit alias because
     * nested collection refs have only a single implicit alias.
     */
    public boolean hasExplicitAlias() {
        return hasExplicitAlias_;
    }

    /**
     * Returns the explicit alias if this table ref has one, null otherwise.
     */
    public String getExplicitAlias() {
        if (hasExplicitAlias()) {
            return getUniqueAlias();
        }
        return null;
    }

    public boolean isResolved() {
        return !getClass().equals(TableRef.class);
    }

    public void reset() {
        isAnalyzed = false;
        //  resolvedPath_ = null;
        if (usingColNames != null) {
            // The using col names are converted into an on-clause predicate during analysis,
            // so unset the on-clause here.
            onClause = null;
        } else if (onClause != null) {
            onClause.reset();
        }
        leftTblRef = null;
        allTableRefIds_.clear();
        allMaterializedTupleIds_.clear();
        correlatedTupleIds_.clear();
        desc = null;
    }

    public boolean isJoinRewrittenFromNotIn() {
        return isJoinRewrittenFromNotIn;
    }

    /**
     * Returns a deep clone of this table ref without also cloning the chain of table refs.
     * Sets leftTblRef_ in the returned clone to null.
     */
    @Override
    public TableRef clone() {
        return new TableRef(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        if (partitionNames != null) {
            sb.append(partitionNames);
        }
        if (aliases_ != null && aliases_.length > 0) {
            sb.append(" AS ").append(aliases_[0]);
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        if (partitionNames == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            partitionNames.write(out);
        }

        if (hasExplicitAlias()) {
            out.writeBoolean(true);
            Text.writeString(out, getExplicitAlias());
        } else {
            out.writeBoolean(false);
        }
    }

    public void readFields(DataInput in) throws IOException {
        name = new TableName();
        name.readFields(in);
        if (in.readBoolean()) {
            if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_77) {
                List<String> partitions = Lists.newArrayList();
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    String partName = Text.readString(in);
                    partitions.add(partName);
                }
                partitionNames = new PartitionNames(false, partitions);
            } else {
                partitionNames = PartitionNames.read(in);
            }
        }

        if (in.readBoolean()) {
            String alias = Text.readString(in);
            aliases_ = new String[] {alias};
        }
    }
}
