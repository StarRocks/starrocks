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

package com.starrocks.planner.expression;

import com.starrocks.thrift.TTypeDesc;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Lets the thrift expr nodes built in the current thread share one {@link TTypeDesc} per distinct thrift form of a
 * complex {@link Type} while the scope is open.
 *
 * <p>Every thrift expr node carries its full type, so without sharing a STRUCT column with tens of thousands of fields
 * that is referenced K times in a fragment gets K copies of its type, about 150 bytes of heap per leaf field each.
 * Sharing the object does not change the serialized bytes, because thrift writes a shared object at every place that
 * references it.
 *
 * <p>A type object is first looked up by identity, so it is converted only once. A type object seen for the first
 * time is converted and then looked up by the value of its thrift form, which covers every serialized field. Type
 * equality is not used: {@code StructType.equals} ignores the case of the field names, the field comments and whether
 * the struct is named, which are all serialized, so two equal types may still serialize differently. Scalar types are
 * not shared, their thrift form is a single small node.
 *
 * <p>A shared {@link TTypeDesc} must not be modified, and the types must not be modified while the scope is open.
 * A nested scope reuses the outer one, and only the outermost scope clears the cache when it is closed.
 */
public final class TypeDescSharingScope implements AutoCloseable {
    private static final ThreadLocal<SharedTypeDescs> SHARED_TYPE_DESCS = new ThreadLocal<>();

    private final boolean outermost;

    private TypeDescSharingScope(boolean outermost) {
        this.outermost = outermost;
    }

    public static TypeDescSharingScope open() {
        if (SHARED_TYPE_DESCS.get() != null) {
            return new TypeDescSharingScope(false);
        }
        SHARED_TYPE_DESCS.set(new SharedTypeDescs());
        return new TypeDescSharingScope(true);
    }

    /**
     * Convert {@code type} to thrift, reusing the {@link TTypeDesc} with the same thrift form in the open scope.
     * Without an open scope, every call builds a new one like {@link TypeSerializer#toThrift(Type)}.
     */
    public static TTypeDesc toThrift(Type type) {
        SharedTypeDescs sharedTypeDescs = SHARED_TYPE_DESCS.get();
        if (sharedTypeDescs == null || !type.isComplexType()) {
            return TypeSerializer.toThrift(type);
        }
        return sharedTypeDescs.get(type);
    }

    @Override
    public void close() {
        if (outermost) {
            SHARED_TYPE_DESCS.remove();
        }
    }

    private static final class SharedTypeDescs {
        private final Map<Type, TTypeDesc> byType = new IdentityHashMap<>();
        private final Map<TTypeDesc, TTypeDesc> byValue = new HashMap<>();

        TTypeDesc get(Type type) {
            return byType.computeIfAbsent(type,
                    newType -> byValue.computeIfAbsent(TypeSerializer.toThrift(newType), Function.identity()));
        }
    }
}
