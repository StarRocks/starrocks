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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class SplitTabletClause extends AlterTableClause {

    private final PartitionNames partitionNames;

    private final TabletList tabletList;

    private final Map<String, String> properties;

    private long dynamicTabletSplitSize;

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

    public void analyze() throws StarRocksException {
        if (partitionNames != null && tabletList != null) {
            throw new StarRocksException("Partitions and tablets cannot be specified at the same time");
        }

        if (partitionNames != null) {
            if (partitionNames.isTemp()) {
                throw new StarRocksException("Cannot split tablet in temp partition");
            }
            if (partitionNames.getPartitionNames().isEmpty()) {
                throw new StarRocksException("Empty partitions");
            }
        }

        if (tabletList != null && tabletList.getTabletIds().isEmpty()) {
            throw new StarRocksException("Empty tablets");
        }

        if (properties == null) {
            dynamicTabletSplitSize = Config.dynamic_tablet_split_size;
            return;
        }

        String splitSize = properties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_TABLET_SPLIT_SIZE);
        try {
            dynamicTabletSplitSize = Long.parseLong(splitSize);
        } catch (Exception e) {
            throw new StarRocksException("Invalid property value: " + splitSize);
        }

        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        copiedProperties.remove(PropertyAnalyzer.PROPERTIES_DYNAMIC_TABLET_SPLIT_SIZE);
        if (!copiedProperties.isEmpty()) {
            throw new StarRocksException("Unknown properties: " + copiedProperties);
        }
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
        return visitor.visitSplitTabletClause(this, context);
    }
}
