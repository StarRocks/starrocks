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

package com.starrocks.sql.ast.expression;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.parser.NodePosition;

/**
 * Legacy table reference class used for metadata serialization and deserialization.
 * This class is currently used by a limited number of components and is planned for
 * gradual migration to the new AST-based TableRef class.
 * <p>
 * Key differences between TableRefPersist and TableRef:
 * <ul>
 *   <li>TableRefPersist: Used for metadata persistence, serialization, and storage operations.
 *       Contains complex analysis state and join specifications for backward compatibility.</li>
 *   <li>TableRef: Pure AST class used for SQL parsing and query planning. Cleaner design
 *       without persistence-related concerns.</li>
 * </ul>
 * <p>
 * Migration Plan:
 * The new TableRef class in com.starrocks.sql.ast.TableRef should gradually replace
 * TableRefPersist usage across the codebase. TableRefPersist will be deprecated once
 * all components have been migrated to use the new AST-based approach.
 * <p>
 * Current Usage:
 * This class is primarily used in backup/restore operations, export jobs, and other
 * metadata persistence scenarios where serialization compatibility is required.
 */
public class TableRefPersist implements ParseNode, Writable {
    @SerializedName(value = "name")
    protected TableName name;
    @SerializedName(value = "partitionNames")
    private PartitionNames partitionNames = null;

    // Legal aliases of this table ref. Contains the explicit alias as its sole element if
    // there is one. Otherwise, contains the two implicit aliases. Implicit aliases are set
    // in the c'tor of the corresponding resolved table ref (subclasses of TableRef) during
    // analysis. By convention, for table refs with multiple implicit aliases, aliases_[0]
    // contains the fully-qualified implicit alias to ensure that aliases_[0] always
    // uniquely identifies this table ref regardless of whether it has an explicit alias.
    @SerializedName(value = "aliases")
    protected String[] aliases;

    // Indicates whether this table ref is given an explicit alias,
    protected boolean hasExplicitAlias;

    protected final NodePosition pos;

    public TableRefPersist() {
        // for persist
        pos = NodePosition.ZERO;
    }

    public TableRefPersist(TableName name, String alias) {
        this(name, alias, null, NodePosition.ZERO);
    }

    public TableRefPersist(TableName name, String alias, PartitionNames partitionNames) {
        this(name, alias, partitionNames, NodePosition.ZERO);
    }

    public TableRefPersist(TableName name, String alias, PartitionNames partitionNames, NodePosition pos) {
        this.pos = pos;
        this.name = name;
        if (alias != null) {
            aliases = new String[] {alias};
            hasExplicitAlias = true;
        } else {
            hasExplicitAlias = false;
        }
        this.partitionNames = partitionNames;
    }

    // Only used to clone
    // this will reset all the 'analyzed' stuff
    protected TableRefPersist(TableRefPersist other) {
        pos = other.pos;
        name = other.name;
        aliases = other.aliases;
        hasExplicitAlias = other.hasExplicitAlias;
        partitionNames = (other.partitionNames != null) ? new PartitionNames(other.partitionNames) : null;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public TableName getName() {
        return name;
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
        return aliases;
    }

    /**
     * Returns the explicit alias or the fully-qualified implicit alias. The returned alias
     * is guaranteed to be unique (i.e., column/field references against the alias cannot
     * be ambiguous).
     */
    public String getUniqueAlias() {
        return aliases[0];
    }

    /**
     * Returns true if this table ref has an explicit alias.
     * Note that getAliases().length() == 1 does not imply an explicit alias because
     * nested collection refs have only a single implicit alias.
     */
    public boolean hasExplicitAlias() {
        return hasExplicitAlias;
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

    /**
     * Returns a deep clone of this table ref without also cloning the chain of table refs.
     * Sets leftTblRef_ in the returned clone to null.
     */
    @Override
    public TableRefPersist clone() {
        return new TableRefPersist(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        if (partitionNames != null) {
            sb.append(partitionNames);
        }
        if (aliases != null && aliases.length > 0) {
            sb.append(" AS ").append(aliases[0]);
        }
        return sb.toString();
    }
}
