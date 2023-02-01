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

package com.starrocks.jni.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SelectedFields {
    private final Set<String> dedup = new HashSet<>();
    private final List<String> fields = new ArrayList<>();
    private Map<String, SelectedFields> children = null;

    // add nested path like 'a.b.c.d' to nested fields
    public void addNestedPath(String path) {
        String[] paths = path.split("\\.");
        addNestedPath(paths, 0);
    }

    public void addMultipleNestedPath(String path) {
        for (String p : path.split(",")) {
            addNestedPath(p);
        }
    }

    public void addNestedPath(String[] paths, int offset) {
        String f = paths[offset];
        if (!dedup.contains(f)) {
            fields.add(f);
            dedup.add(f);
        }
        if ((offset + 1) < paths.length) {
            if (children == null) {
                children = new HashMap<>();
            }
            if (!children.containsKey(f)) {
                SelectedFields sub = new SelectedFields();
                children.put(f, sub);
            }
            children.get(f).addNestedPath(paths, offset + 1);
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public SelectedFields findChildren(String f) {
        return children == null ? null : children.get(f);
    }
}
