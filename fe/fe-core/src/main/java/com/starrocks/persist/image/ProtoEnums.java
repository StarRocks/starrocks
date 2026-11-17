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

import com.google.protobuf.ProtocolMessageEnum;

/**
 * Maps Java (and Thrift) enums to the proto enums that mirror them, by constant name.
 *
 * <p>GSON persists enums by name, and the proto enums in the persist protos are declared with the same names, so a
 * by-name mapping keeps the two formats interchangeable without a hand-written switch per enum. A name that is missing
 * on either side is a programming error (a constant was added to one side only) or an image written by a newer
 * version, and both fail fast instead of silently loading a wrong value.
 */
public final class ProtoEnums {
    private ProtoEnums() {
    }

    public static <P extends Enum<P> & ProtocolMessageEnum> P toProto(Enum<?> value, Class<P> protoEnum) {
        return toProto(value, protoEnum, "");
    }

    /**
     * For package-level proto enums, whose values carry the enum name as a prefix (e.g. {@code KEYS_TYPE_DUP_KEYS}
     * for {@code KeysType.DUP_KEYS}).
     */
    public static <P extends Enum<P> & ProtocolMessageEnum> P toProto(Enum<?> value, Class<P> protoEnum,
                                                                        String prefix) {
        try {
            return Enum.valueOf(protoEnum, prefix + value.name());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(protoEnum.getName() + " has no value named " + prefix + value.name()
                    + " for " + value.getDeclaringClass().getName(), e);
        }
    }

    public static <J extends Enum<J>> J fromProto(ProtocolMessageEnum value, Class<J> javaEnum) {
        return fromProto(value, javaEnum, "");
    }

    public static <J extends Enum<J>> J fromProto(ProtocolMessageEnum value, Class<J> javaEnum, String prefix) {
        String name = ((Enum<?>) value).name();
        if ("UNRECOGNIZED".equals(name) || name.endsWith("UNSPECIFIED") || !name.startsWith(prefix)) {
            throw new IllegalArgumentException("no " + javaEnum.getName() + " for proto value " + name
                    + " of " + value.getClass().getName());
        }
        try {
            return Enum.valueOf(javaEnum, name.substring(prefix.length()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(javaEnum.getName() + " has no value named " + name
                    + " for " + value.getClass().getName(), e);
        }
    }
}
