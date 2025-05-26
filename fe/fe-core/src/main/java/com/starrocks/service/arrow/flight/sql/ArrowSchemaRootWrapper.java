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

package com.starrocks.service.arrow.flight.sql;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * A wrapper class for Apache Arrow's {@link VectorSchemaRoot} that ensures
 * proper release of off-heap memory.
 * <p>
 * This class is intended for use in Arrow-specific contexts, such as
 * caching or passing {@link VectorSchemaRoot} instances, where memory
 * must be explicitly released to avoid memory leaks.
 * <p>
 * Always call {@link #close()} when the wrapper is no longer needed.
 * This class implements {@link AutoCloseable} to support use with
 * try-with-resources.
 */
public class ArrowSchemaRootWrapper implements AutoCloseable {
    private final VectorSchemaRoot schemaRoot;

    public ArrowSchemaRootWrapper(VectorSchemaRoot schemaRoot) {
        this.schemaRoot = schemaRoot;
    }

    public VectorSchemaRoot getSchemaRoot() {
        return schemaRoot;
    }

    /**
     * Releases the off-heap memory associated with the wrapped {@link VectorSchemaRoot}.
     * <p>
     * This method must be called to avoid memory leaks.
     */
    @Override
    public void close() {
        if (schemaRoot != null) {
            schemaRoot.close();
        }
    }
}