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

package com.starrocks.lake.changes;

import com.starrocks.catalog.Column;
import com.starrocks.thrift.TChangesMetaDescriptor;
import com.starrocks.thrift.TChangesMetaKind;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/** One CHANGES metadata column resolved against a single relation's schema. */
public record ChangesMetaDescriptor(TChangesMetaKind kind, String name, Type type, boolean isNullable) {

    // One default descriptor per kind, in declaration order.
    // resolve() reuses an entry directly when its name is free, or mints a
    // copy with an alternate name when the default collides.
    private static final List<ChangesMetaDescriptor> DEFAULTS = List.of(
            new ChangesMetaDescriptor(TChangesMetaKind.CHANGE_TYPE, "__CHANGE_TYPE__", IntegerType.TINYINT, true),
            new ChangesMetaDescriptor(TChangesMetaKind.ROW_VERSION, "__ROW_VERSION__", IntegerType.BIGINT, true));

    /**
     * Returns one descriptor per kind for a relation whose base columns are
     * {@code tableSchema}. A kind's default name is reused when no base column
     * collides; otherwise the alternate is built by inserting {@code _<n>}
     * before the trailing double underscores, picking the lowest {@code n >= 1}
     * that is still free. Collision detection is case-insensitive to match
     * StarRocks column resolution, and the walk is deterministic so two
     * resolutions on the same schema agree.
     *
     * <p>Each kind appears exactly once, in {@link #DEFAULTS} declaration order.
     */
    public static List<ChangesMetaDescriptor> resolve(Collection<Column> tableSchema) {
        TreeSet<String> occupiedNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : tableSchema) {
            occupiedNames.add(column.getName());
        }

        List<ChangesMetaDescriptor> descriptors = new ArrayList<>(DEFAULTS.size());
        for (ChangesMetaDescriptor def : DEFAULTS) {
            String name = chooseAvailableQueryName(def.name(), occupiedNames);
            occupiedNames.add(name);
            descriptors.add(name.equals(def.name())
                    ? def
                    : new ChangesMetaDescriptor(def.kind(), name, def.type(), def.isNullable()));
        }
        return descriptors;
    }

    private static String chooseAvailableQueryName(String defaultQueryName, TreeSet<String> occupiedNames) {
        if (!occupiedNames.contains(defaultQueryName)) {
            return defaultQueryName;
        }
        // Inject _<n> inside the trailing "__" so the alternate keeps the
        // same "__FOO_..._" shape and stays visually distinguishable from a
        // user column. Plain suffix only when the default lacks that shape.
        boolean hasTrailingDoubleUnderscore = defaultQueryName.endsWith("__");
        String prefix = hasTrailingDoubleUnderscore
                ? defaultQueryName.substring(0, defaultQueryName.length() - 2)
                : defaultQueryName;
        String suffix = hasTrailingDoubleUnderscore ? "__" : "";
        for (int i = 1; ; i++) {
            String candidate = prefix + "_" + i + suffix;
            if (!occupiedNames.contains(candidate)) {
                return candidate;
            }
        }
    }

    public TChangesMetaDescriptor toThrift() {
        TChangesMetaDescriptor thrift = new TChangesMetaDescriptor();
        thrift.setKind(kind);
        thrift.setName(name);
        thrift.setType(TypeSerializer.toThrift(type));
        thrift.setIs_nullable(isNullable);
        return thrift;
    }
}
