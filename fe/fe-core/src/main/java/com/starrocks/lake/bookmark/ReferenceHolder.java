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

package com.starrocks.lake.bookmark;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MvId;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;

import java.util.Objects;

/**
 * Identifies an entity that references a bookmark. Two holders are equal when
 * their {@code type} and {@code name} match.
 */
public abstract class ReferenceHolder {

    public enum Type {
        /** A materialized view holding a base-table bookmark for its refresh logic. */
        MATERIALIZED_VIEW,
        /** Any other holder, identified by a caller-chosen name. */
        CUSTOM
    }

    @SerializedName("t")
    private final Type type;

    protected ReferenceHolder(Type type) {
        this.type = Objects.requireNonNull(type);
    }

    public Type getType() {
        return type;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReferenceHolder)) {
            return false;
        }
        ReferenceHolder other = (ReferenceHolder) o;
        return type == other.type && Objects.equals(name(), other.name());
    }

    @Override
    public final int hashCode() {
        return Objects.hash(type, name());
    }

    /** Distinguishes holders of the same type. */
    protected abstract Object name();

    public static RuntimeTypeAdapterFactory<ReferenceHolder> typeAdapterFactory() {
        return RuntimeTypeAdapterFactory.of(ReferenceHolder.class, "clazz")
                .registerSubtype(Mv.class, "mv")
                .registerSubtype(Custom.class, "c");
    }

    /* ---------- subclasses ---------- */

    public static final class Mv extends ReferenceHolder {
        @SerializedName("mv")
        private final MvId mvId;

        public Mv(MvId mvId) {
            super(Type.MATERIALIZED_VIEW);
            this.mvId = Objects.requireNonNull(mvId);
        }

        public MvId getMvId() {
            return mvId;
        }

        @Override
        protected Object name() {
            return mvId;
        }

        @Override
        public String toString() {
            return String.format("%s:%s-%s", Type.MATERIALIZED_VIEW.name(),
                    mvId.getDbId(), mvId.getId());
        }
    }

    public static final class Custom extends ReferenceHolder {
        @SerializedName("n")
        private final String name;

        public Custom(String name) {
            super(Type.CUSTOM);
            this.name = Objects.requireNonNull(name);
        }

        @Override
        protected Object name() {
            return name;
        }

        @Override
        public String toString() {
            return Type.CUSTOM.name() + ":" + name;
        }
    }
}
