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

package com.starrocks.catalog;

import com.google.api.client.util.Lists;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TColumnAccessPath;

import java.util.List;
import java.util.stream.Collectors;

public class ColumnAccessPath {
    private final TAccessPathType type;

    private final String path;

    private final List<ColumnAccessPath> children;

    public ColumnAccessPath(TAccessPathType type, String path) {
        this.type = type;
        this.path = path;
        this.children = Lists.newArrayList();
    }

    public boolean hasChildPath(String path) {
        return children.stream().anyMatch(p -> p.path.equals(path));
    }

    public void addChildPath(ColumnAccessPath child) {
        children.add(child);
    }

    public ColumnAccessPath getChildPath(String path) {
        return children.stream().filter(p -> p.path.equals(path)).findFirst().orElse(null);
    }

    public void clearChildPath() {
        children.clear();
    }

    private void toStringImpl(ColumnAccessPath path, List<String> allPaths) {
        for (String parentPath : allPaths) {
            allPaths.add(parentPath + "/" + path);
        }

        for (ColumnAccessPath child : children) {
            toStringImpl(child, allPaths);
        }
    }

    @Override
    public String toString() {
        List<String> path = Lists.newArrayList();
        path.add("");
        toStringImpl(this, path);
        return "[" + String.join(", ", path) + "]";
    }

    public TColumnAccessPath toThrift() {
        TColumnAccessPath tColumnAccessPath = new TColumnAccessPath();
        tColumnAccessPath.setType(type);
        tColumnAccessPath.setPath(new StringLiteral(path).treeToThrift());
        tColumnAccessPath.setChildren(children.stream().map(ColumnAccessPath::toThrift).collect(Collectors.toList()));
        return tColumnAccessPath;
    }
}