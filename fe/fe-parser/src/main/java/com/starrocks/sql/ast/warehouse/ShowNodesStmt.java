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
// See the License for the specific

package com.starrocks.sql.ast.warehouse;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

// Show nodes from warehouse statement
public class ShowNodesStmt extends ShowStmt {
    private final String pattern;
    private final String warehouseName;
    private final String cnGroupName;

    public ShowNodesStmt(String warehouseName, String cnGroupName, String pattern, NodePosition pos) {
        super(pos);
        this.warehouseName = warehouseName;
        this.pattern = pattern;
        this.cnGroupName = cnGroupName;
    }

    public String getPattern() {
        return pattern;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public String getCnGroupName() {
        return cnGroupName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowNodesStatement(this, context);
    }
}