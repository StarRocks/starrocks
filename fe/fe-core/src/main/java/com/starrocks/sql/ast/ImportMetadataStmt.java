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

import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// The INCLUDE METADATA (<key> AS <alias>, ...) clause of a routine-load statement. Each item binds a
// source-metadata key (KEY, PARTITION, OFFSET, TIMESTAMP_MS, HEADERS, MESSAGE_ID, ...) to a hidden
// source column named <alias> that COLUMNS exprs reference. The key is the raw text; it is validated
// against the data source and mapped to a metadata kind in the analyzer
// (see RoutineLoadMetadata.validateIncludeMetadata).
public class ImportMetadataStmt extends StatementBase {
    public static class Item {
        private final String key;
        private final String alias;
        private final NodePosition pos;

        public Item(String key, String alias, NodePosition pos) {
            this.key = key;
            this.alias = alias;
            this.pos = pos;
        }

        public String getKey() {
            return key;
        }

        public String getAlias() {
            return alias;
        }

        public NodePosition getPos() {
            return pos;
        }
    }

    private final List<Item> items;

    public ImportMetadataStmt(List<Item> items) {
        this(items, NodePosition.ZERO);
    }

    public ImportMetadataStmt(List<Item> items, NodePosition pos) {
        super(pos);
        this.items = items;
    }

    public List<Item> getItems() {
        return items;
    }

    // Renders the clause as it appears in CREATE ROUTINE LOAD, e.g. INCLUDE METADATA(KEY AS `k`, ...).
    // Both RoutineLoadDesc.toSql (origStmt persistence) and SHOW CREATE ROUTINE LOAD render through this
    // single helper so the persisted and shown forms cannot drift. Keys are validated keywords emitted
    // as-is; the alias is rendered with the shared ParseUtil.backquote helper, so a reserved-word alias
    // re-parses to the same name.
    public String toSql() {
        StringBuilder sb = new StringBuilder("INCLUDE METADATA(");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Item item = items.get(i);
            sb.append(item.getKey()).append(" AS ").append(ParseUtil.backquote(item.getAlias()));
        }
        sb.append(")");
        return sb.toString();
    }
}
