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
 *      TABLETS (id1, id2) (id3, id4)
 */
public class TabletGroupList implements ParseNode {

    private final List<List<Long>> tabletIdGroups;

    private final NodePosition pos;

    public TabletGroupList(List<List<Long>> tabletIdGroups) {
        this(tabletIdGroups, NodePosition.ZERO);
    }

    public TabletGroupList(List<List<Long>> tabletIdGroups, NodePosition pos) {
        this.tabletIdGroups = tabletIdGroups;
        this.pos = pos;
    }

    public List<List<Long>> getTabletIdGroups() {
        return tabletIdGroups;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TABLETS ");
        for (int i = 0; i < tabletIdGroups.size(); i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append('(').append(Joiner.on(", ").join(tabletIdGroups.get(i))).append(')');
        }
        return sb.toString();
    }
}
