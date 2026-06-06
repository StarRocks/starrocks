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

import com.google.common.base.Joiner;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/*
 * To represent following stmt:
 *      TABLET id1
 *      TABLET (id1, id2)
 *      TABLETS (id1, id2)
 */
public class TabletList implements ParseNode {

    private final List<Long> tabletIds;

    private final NodePosition pos;

    public TabletList(List<Long> tabletIds) {
        this(tabletIds, NodePosition.ZERO);
    }

    public TabletList(List<Long> tabletIds, NodePosition pos) {
        this.tabletIds = tabletIds;
        this.pos = pos;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TABLETS (");
        sb.append(Joiner.on(", ").join(tabletIds));
        sb.append(")");
        return sb.toString();
    }
}
