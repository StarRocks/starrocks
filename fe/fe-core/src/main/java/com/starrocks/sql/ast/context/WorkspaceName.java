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

package com.starrocks.sql.ast.context;

import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

/**
 * Fully-qualified workspace name: {@code contextbase.collection.workspace}.
 * contextbase/collection may be null when resolved from session state later.
 */
public class WorkspaceName {

    private final String contextBase;
    private final String collection;
    private final String workspace;
    private final NodePosition pos;

    public WorkspaceName(String contextBase, String collection, String workspace, NodePosition pos) {
        this.contextBase = contextBase;
        this.collection = collection;
        this.workspace = workspace;
        this.pos = pos;
    }

    public String getContextBase() {
        return contextBase;
    }

    public String getCollection() {
        return collection;
    }

    public String getWorkspace() {
        return workspace;
    }

    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (contextBase != null) {
            sb.append(contextBase).append('.');
        }
        if (collection != null) {
            sb.append(collection).append('.');
        }
        sb.append(workspace);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WorkspaceName)) {
            return false;
        }
        WorkspaceName other = (WorkspaceName) o;
        return Objects.equals(contextBase, other.contextBase)
                && Objects.equals(collection, other.collection)
                && Objects.equals(workspace, other.workspace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextBase, collection, workspace);
    }
}
