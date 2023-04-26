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

import com.starrocks.alter.AlterOpType;
import com.starrocks.load.LoadErrorHub;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// FORMAT:
//   ALTER SYSTEM SET LOAD ERRORS HUB properties("type" = "xxx");

@Deprecated
public class AlterLoadErrorUrlClause extends AlterClause {
    private final Map<String, String> properties;
    private LoadErrorHub.Param param;

    public AlterLoadErrorUrlClause(Map<String, String> properties) {
        this(properties, NodePosition.ZERO);
    }

    public AlterLoadErrorUrlClause(Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.ALTER_OTHER, pos);
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
