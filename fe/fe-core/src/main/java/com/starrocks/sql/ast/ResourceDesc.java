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
import com.starrocks.common.util.PrintableMap;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// Resource descriptor
//
// Spark example:
// WITH RESOURCE "spark0"
// (
//   "spark.jars" = "xxx.jar,yyy.jar",
//   "spark.files" = "/tmp/aaa,/tmp/bbb",
//   "spark.executor.memory" = "1g",
//   "spark.yarn.queue" = "queue0"
// )
public class ResourceDesc implements ParseNode {
    protected String name;
    protected Map<String, String> properties;

    protected NodePosition pos;

    public ResourceDesc(String name, Map<String, String> properties) {
        this(name, properties, NodePosition.ZERO);
    }

    public ResourceDesc(String name, Map<String, String> properties, NodePosition pos) {
        this.pos = pos;
        this.name = name;
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newHashMap();
        }
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(" WITH RESOURCE '").append(name).append("'");
        if (properties != null && !properties.isEmpty()) {
            PrintableMap<String, String> printableMap = new PrintableMap<>(properties, " = ", true, false, true);
            sb.append(" (").append(printableMap.toString()).append(")");
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
