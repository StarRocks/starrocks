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

import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;
import java.util.Map;

public class AlterTaskStmt extends DdlStmt {

    public enum AlterAction {
        RESUME,
        SUSPEND,
        SET
    }

    private final TaskName taskName;
    private final boolean ifExists;
    private final AlterAction action;
    private final Map<String, String> properties;

    public AlterTaskStmt(TaskName taskName, boolean ifExists, AlterAction action,
                         Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.taskName = taskName;
        this.ifExists = ifExists;
        this.action = action;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    public TaskName getTaskName() {
        return taskName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public AlterAction getAction() {
        return action;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTaskStatement(this, context);
    }
}
