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

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

public class UnitIdentifier implements ParseNode {

    public static final UnitIdentifier SECOND_UNIT = new UnitIdentifier("SECOND");
    public static final UnitIdentifier MINUTE_UNIT = new UnitIdentifier("MINUTE");
    public static final UnitIdentifier HOUR_UNIT = new UnitIdentifier("HOUR");
    public static final UnitIdentifier DAY_UNIT = new UnitIdentifier("DAY");

    private final String description;

    private final NodePosition pos;

    public UnitIdentifier(String description) {
        this(description, NodePosition.ZERO);
    }

    public UnitIdentifier(String description, NodePosition pos) {
        this.pos = pos;
        this.description = description.toUpperCase();
    }

    public String getDescription() {
        return description;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toSql() {
        return description;
    }
}
