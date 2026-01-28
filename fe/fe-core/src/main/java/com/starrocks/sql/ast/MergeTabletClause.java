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

import com.starrocks.common.Config;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class MergeTabletClause extends AlterTableClause {

    private final PartitionRef partitionNames;

    private final TabletGroupList tabletGroupList;

    private final Map<String, String> properties;

    private long tabletReshardTargetSize;

    public MergeTabletClause() {
        this(null, null, null);
        this.tabletReshardTargetSize = Config.tablet_reshard_target_size;
    }

    public MergeTabletClause(
            PartitionRef partitionNames,
            TabletGroupList tabletGroupList,
            Map<String, String> properties) {
        this(partitionNames, tabletGroupList, properties, NodePosition.ZERO);
    }

    public MergeTabletClause(
            PartitionRef partitionNames,
            TabletGroupList tabletGroupList,
            Map<String, String> properties,
            NodePosition pos) {
        super(pos);
        this.partitionNames = partitionNames;
        this.tabletGroupList = tabletGroupList;
        this.properties = properties;
    }

    public PartitionRef getPartitionNames() {
        return partitionNames;
    }

    public TabletGroupList getTabletGroupList() {
        return tabletGroupList;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getTabletReshardTargetSize() {
        return tabletReshardTargetSize;
    }

    public void setTabletReshardTargetSize(long tabletReshardTargetSize) {
        this.tabletReshardTargetSize = tabletReshardTargetSize;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (partitionNames != null) {
            sb.append("MERGE TABLET ");
            sb.append(partitionNames.toString());
            sb.append('\n');
        } else if (tabletGroupList != null) {
            sb.append("MERGE ");
            sb.append(tabletGroupList.toString());
            sb.append('\n');
        } else {
            sb.append("MERGE TABLET\n");
        }
        if (properties != null && !properties.isEmpty()) {
            sb.append("PROPERTIES (\n").append(new PrintableMap<>(properties, "=", true, true)).append(")");
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitMergeTabletClause(this, context);
    }
}
