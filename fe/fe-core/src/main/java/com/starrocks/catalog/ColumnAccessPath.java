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

/*
 * ColumnAccessPath is used to describe the access path of a complex(Map/Struct/Json) column.
 *
 * eg:
 *  select struct_a.col_b.col_c.col_d
 *
 * ColumnAccessPath will be:
 *  struct_a (ROOT)
 *  -- col_b (FIELD)
 *    -- col_c (FIELD)
 *      -- col_d (FIELD)
 *
 * in complex sql, eg:
 *  select  struct_a.col_g, struct_a.col_b.col_c.col_d, struct_a.col_b.col_e.col_f
 * ColumnAccessPath will be:
 *  struct_a (ROOT)
 *  -- col_g (FIELD)
 *  -- col_b (FIELD)
 *    -- col_c (FIELD)
 *      -- col_d (FIELD)
 *    -- col_e (FIELD)
 *      -- col_f (FIELD)
 *
 */
public class ColumnAccessPath {
    public static final String PATH_PLACEHOLDER = "P";
    // The top one must be ROOT
    private TAccessPathType type;

    private String path;

    private final List<ColumnAccessPath> children;

    private boolean fromPredicate;

    public ColumnAccessPath(TAccessPathType type, String path) {
        this.type = type;
        this.path = path;
        this.children = Lists.newArrayList();
        this.fromPredicate = false;
    }

    public void setType(TAccessPathType type) {
        this.type = type;
    }

    public TAccessPathType getType() {
        return type;
    }

    public String getPath() {
        return path;
    }

    public boolean onlyRoot() {
        return type == TAccessPathType.ROOT && children.isEmpty();
    }

    public void setFromPredicate(boolean fromPredicate) {
        this.fromPredicate = fromPredicate;
    }

    public boolean isFromPredicate() {
        return fromPredicate;
    }

    public boolean hasChildPath(String path) {
        return children.stream().anyMatch(p -> p.path.equals(path));
    }

    public boolean hasOverlap(List<String> fieldNames) {
        if (!hasChildPath() || fieldNames.isEmpty()) {
            return true;
        }

        if (children.stream().noneMatch(p -> p.path.equals(fieldNames.get(0)))) {
            return false;
        }
        return getChildPath(fieldNames.get(0)).hasOverlap(fieldNames.subList(1, fieldNames.size()));
    }

    public boolean hasChildPath() {
        return !children.isEmpty();
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

    private void explainImpl(String parent, List<String> allPaths) {
        boolean hasName = type == TAccessPathType.FIELD || type == TAccessPathType.ROOT;
        String cur = parent + "/" + (hasName ? path : type.name());
        if (children.isEmpty()) {
            allPaths.add(cur);
        }
        for (ColumnAccessPath child : children) {
            child.explainImpl(cur, allPaths);
        }
    }

    public String explain() {
        List<String> allPaths = Lists.newArrayList();
        explainImpl("", allPaths);
        allPaths.sort(String::compareTo);
        return String.join(", ", allPaths);
    }

    @Override
    public String toString() {
        return path;
    }

    public TColumnAccessPath toThrift() {
        TColumnAccessPath tColumnAccessPath = new TColumnAccessPath();
        tColumnAccessPath.setType(type);
        tColumnAccessPath.setPath(new StringLiteral(path).treeToThrift());
        tColumnAccessPath.setChildren(children.stream().map(ColumnAccessPath::toThrift).collect(Collectors.toList()));
        tColumnAccessPath.setFrom_predicate(fromPredicate);
        return tColumnAccessPath;
    }
}