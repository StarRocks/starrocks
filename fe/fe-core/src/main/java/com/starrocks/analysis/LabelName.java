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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/LabelName.java

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

import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// label name used to identify a load job
public class LabelName implements ParseNode, Writable {
    private String dbName;
    private String labelName;

    private final NodePosition pos;

    public LabelName() {
        pos = NodePosition.ZERO;
    }

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

    public void analyze(ConnectContext context) {
        dbName = AnalyzerUtils.getOrDefaultDatabase(dbName, context);
        FeNameFormat.checkLabel(labelName);
    }

    @Override
    public boolean equals(Object rhs) {
        if (this == rhs) {
            return true;
        }
        if (rhs instanceof LabelName) {
            LabelName rhsLabel = (LabelName) rhs;
            return this.dbName.equals(rhsLabel.dbName) && this.labelName.equals(rhsLabel.labelName);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(dbName).append(labelName).toHashCode();
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("`").append(dbName).append("`.`").append(labelName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // compatible with old version
        Text.writeString(out, ClusterNamespace.getFullName(dbName));
        Text.writeString(out, labelName);
    }

    public void readFields(DataInput in) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_30) {
            dbName = Text.readString(in);
        } else {
            dbName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        }
        labelName = Text.readString(in);
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
