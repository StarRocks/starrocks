// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/AdminShowConfigStmt.java

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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.AdminSetConfigStmt.ConfigType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
<<<<<<< HEAD
import com.starrocks.server.GlobalStateMgr;
=======
import com.starrocks.sql.ast.AstVisitor;
>>>>>>> cf0fb08a7 ([Refactor] Remove some unused code in old parser for admin stmt (#8963))

// admin show frontend config;
public class AdminShowConfigStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>().add("Key").add(
            "Value").add("Type").add("IsMutable").add("Comment").build();

    private ConfigType type;

    private String pattern;

    public AdminShowConfigStmt(ConfigType type, String pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public ConfigType getType() {
        return type;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
