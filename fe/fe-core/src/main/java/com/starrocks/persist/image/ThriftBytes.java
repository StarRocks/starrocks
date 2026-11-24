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

package com.starrocks.persist.image;

import com.google.protobuf.ByteString;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * A Thrift struct that a persisted class carries as it is (the payload of a BE request, typically) travels in the
 * image as Thrift compact bytes, so the proto does not restate the Thrift definition.
 */
public final class ThriftBytes {
    private ThriftBytes() {
    }

    public static ByteString toBytes(TBase<?, ?> value) {
        try {
            return ByteString.copyFrom(new TSerializer(new TCompactProtocol.Factory()).serialize(value));
        } catch (TException e) {
            throw new IllegalStateException("cannot serialize " + value.getClass().getSimpleName(), e);
        }
    }

    public static <T extends TBase<?, ?>> T fromBytes(ByteString bytes, T target) {
        try {
            new TDeserializer(new TCompactProtocol.Factory()).deserialize(target, bytes.toByteArray());
            return target;
        } catch (TException e) {
            throw new IllegalStateException("cannot deserialize " + target.getClass().getSimpleName(), e);
        }
    }
}
