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

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

// Show ResourceGroups
// 1. Show ResourceGroup specified by name
//  SHOW RESOURCE GROUP <name>
// 2. Show all ResourceGroups
//  SHOW RESOURCE GROUPS ALL
// 3. Show all of ResourceGroups that visible to current user
//  SHOW RESOURCE GROUPS

public class ShowResourceGroupStmt extends ShowStmt {
    private final String name;
    private final boolean listAll;

    public ShowResourceGroupStmt(String name, boolean listAll) {
        this(name, listAll, NodePosition.ZERO);
    }

    public ShowResourceGroupStmt(String name, boolean listAll, NodePosition pos) {
        super(pos);
        this.name = name;
        this.listAll = listAll;
    }

    public boolean isListAll() {
        return listAll;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return ResourceGroup.META_DATA;
    }

    public String getName() {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowResourceGroupStatement(this, context);
    }
}
