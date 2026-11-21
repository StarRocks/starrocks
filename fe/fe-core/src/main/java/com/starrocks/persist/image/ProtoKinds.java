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
import com.starrocks.persist.gson.SubtypeNotFoundException;

/** Validates polymorphic discriminators before converting any common fields or subtype payload. */
public final class ProtoKinds {
    private ProtoKinds() {
    }

    /**
     * Unknown subtype numbers can be skipped by the image reader. A missing discriminator is malformed
     * metadata and must fail regardless of that policy. Ordinary state enums belong in {@link ProtoEnums}.
     */
    public static <K extends Enum<K> & ProtocolMessageEnum> void checkKnown(K kind, int number) {
        if (kind.name().equals("UNRECOGNIZED")) {
            // getNumber() throws on UNRECOGNIZED; keep the number supplied by the message accessor.
            throw new SubtypeNotFoundException(Integer.toString(number),
                    "unknown " + kind.getDeclaringClass().getName() + " value " + number);
        }
        if (number == 0) {
            throw new IllegalArgumentException("missing " + kind.getDeclaringClass().getName());
        }
    }
}
