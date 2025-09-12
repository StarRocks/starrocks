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

public class SplitTabletClause extends AlterTableClause {

    private final PartitionNames partitionNames;

    private final TabletList tabletList;

    private final Map<String, String> properties;

    private long dynamicTabletSplitSize;

    public SplitTabletClause() {
        this(null, null, null);
        this.dynamicTabletSplitSize = Config.dynamic_tablet_split_size;
    }

    public SplitTabletClause(
            PartitionNames partitionNames,
            TabletList tabletList,
            Map<String, String> properties) {
        this(partitionNames, tabletList, properties, NodePosition.ZERO);
    }

    public SplitTabletClause(
            PartitionNames partitionNames,
            TabletList tabletList,
            Map<String, String> properties,
            NodePosition pos) {
        super(pos);
        this.partitionNames = partitionNames;
        this.tabletList = tabletList;
        this.properties = properties;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public TabletList getTabletList() {
        return tabletList;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getDynamicTabletSplitSize() {
        return dynamicTabletSplitSize;
    }

    public void setDynamicTabletSplitSize(long dynamicTabletSplitSize) {
        this.dynamicTabletSplitSize = dynamicTabletSplitSize;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SPLIT TABLET\n");
        if (partitionNames != null) {
            sb.append(partitionNames.toString());
            sb.append('\n');
        }
        if (tabletList != null) {
            sb.append(tabletList.toString());
            sb.append('\n');
        }
        if (properties != null && !properties.isEmpty()) {
            sb.append("PROPERTIES (\n").append(new PrintableMap<>(properties, "=", true, true)).append(")");
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitSplitTabletClause(this, context);
    }
}
