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

import com.google.common.collect.Lists;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.KeysType;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class KeysDesc implements ParseNode {
    private final KeysType type;
    private final List<String> keysColumnNames;
    private final NodePosition pos;

    public KeysDesc() {
        pos = NodePosition.ZERO;
        this.type = KeysType.AGG_KEYS;
        this.keysColumnNames = Lists.newArrayList();
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames) {
        this(type, keysColumnNames, NodePosition.ZERO);
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames, NodePosition pos) {
        this.pos = pos;
        this.type = type;
        this.keysColumnNames = keysColumnNames;
    }

    public KeysType getKeysType() {
        return type;
    }

    public List<String> getKeysColumnNames() {
        return keysColumnNames;
    }

    public boolean containsCol(String colName) {
        return keysColumnNames.stream().anyMatch(e -> StringUtils.equalsIgnoreCase(e, colName));
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}

