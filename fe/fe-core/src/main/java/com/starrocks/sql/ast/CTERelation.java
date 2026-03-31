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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class CTERelation extends Relation {
    public enum CTEMaterializationHint {
        NONE,
        MATERIALIZED,
        NOT_MATERIALIZED
    }

    private final int cteMouldId;
    private final String name;
    private final QueryStatement cteQueryStatement;
    private boolean resolvedInFromClause;
    private int refs = 0; // consume refs
<<<<<<< HEAD

    public CTERelation(int cteMouldId, String name, List<String> columnOutputNames,
                       QueryStatement cteQueryStatement) {
        this(cteMouldId, name, columnOutputNames, cteQueryStatement, NodePosition.ZERO);
    }

    public CTERelation(int cteMouldId, String name, List<String> columnOutputNames,
                       QueryStatement cteQueryStatement, NodePosition pos) {
=======
    private boolean isRecursive;
    private final CTEMaterializationHint materializationHint;

    public CTERelation(int cteMouldId, String name, List<String> columnOutputNames,
                       QueryStatement cteQueryStatement, boolean isRecursive, boolean isAnchor) {
        this(cteMouldId, name, columnOutputNames, cteQueryStatement, isRecursive, isAnchor, NodePosition.ZERO,
                CTEMaterializationHint.NONE);
    }

    public CTERelation(int cteMouldId, String name, List<String> columnOutputNames, QueryStatement cteQueryStatement,
                       boolean isRecursive, boolean isAnchor, NodePosition pos,
                       CTEMaterializationHint materializationHint) {
>>>>>>> 9791a17b75 ([Enhancement] Support materialization hints for CTEs (#70802))
        super(pos);
        this.cteMouldId = cteMouldId;
        this.name = name;
        this.explicitColumnNames = columnOutputNames;
        this.cteQueryStatement = cteQueryStatement;
        this.refs = 0;
<<<<<<< HEAD
=======
        this.isRecursive = isRecursive;
        this.isAnchor = isAnchor;
        this.materializationHint = materializationHint;
>>>>>>> 9791a17b75 ([Enhancement] Support materialization hints for CTEs (#70802))
    }

    public int getCteMouldId() {
        return cteMouldId;
    }

    public QueryStatement getCteQueryStatement() {
        return cteQueryStatement;
    }

    public String getName() {
        return name;
    }

    public void setResolvedInFromClause(boolean resolvedInFromClause) {
        this.resolvedInFromClause = resolvedInFromClause;
    }

    public void addTableRef() {
        refs++;
    }

    public int getRefs() {
        return refs;
    }

    public boolean isResolvedInFromClause() {
        return resolvedInFromClause;
    }

<<<<<<< HEAD
=======
    public void setRecursive(boolean recursive) {
        isRecursive = recursive;
    }

    public boolean isRecursive() {
        return isRecursive;
    }

    public boolean isAnchor() {
        return isAnchor;
    }

    public CTEMaterializationHint getMaterializationHint() {
        return materializationHint;
    }

>>>>>>> 9791a17b75 ([Enhancement] Support materialization hints for CTEs (#70802))
    @Override
    public String toString() {
        return name == null ? String.valueOf(cteMouldId) : name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCTE(this, context);
    }

    @Override
    public TableName getResolveTableName() {
        if (alias != null) {
            return alias;
        } else {
            return new TableName(null, name);
        }
    }
}
