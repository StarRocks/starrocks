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
 * Fully-qualified collection name: {@code contextbase.collection}.
 * The contextbase portion may be null when resolved from session state later.
 */
public class ContextCollectionName {

    private final String contextBase;
    private final String collection;
    private final NodePosition pos;

    public ContextCollectionName(String contextBase, String collection, NodePosition pos) {
        this.contextBase = contextBase;
        this.collection = collection;
        this.pos = pos;
    }

    public String getContextBase() {
        return contextBase;
    }

    public String getCollection() {
        return collection;
    }

    public NodePosition getPos() {
        return pos;
    }

    @Override
    public String toString() {
        return contextBase == null ? collection : contextBase + "." + collection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContextCollectionName)) {
            return false;
        }
        ContextCollectionName other = (ContextCollectionName) o;
        return Objects.equals(contextBase, other.contextBase) && Objects.equals(collection, other.collection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contextBase, collection);
    }
}
