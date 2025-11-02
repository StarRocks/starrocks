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

import java.util.Objects;


// label name used to identify a load job
public class LabelName implements ParseNode {
    private String dbName;
    private final String labelName;

    private final NodePosition pos;

    public LabelName(String dbName, String labelName) {
        this(dbName, labelName, NodePosition.ZERO);
    }

    public LabelName(String dbName, String labelName, NodePosition pos) {
        this.pos = pos;
        this.dbName = dbName;
        this.labelName = labelName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getLabelName() {
        return labelName;
    }

    public String toString() {
        return "`" + dbName + "`.`" + labelName + "`";
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LabelName labelName1 = (LabelName) o;
        return Objects.equals(dbName, labelName1.dbName) && Objects.equals(labelName, labelName1.labelName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, labelName);
    }
}
