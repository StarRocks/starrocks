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

import com.starrocks.persist.proto.Int32ListPB;
import com.starrocks.persist.proto.Int64ListPB;
import com.starrocks.persist.proto.StringListPB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The wrapper messages for Java lists that can be null. The caller decides the null case: a null list is left
 * unset, an unset field is read back as null.
 */
public final class ProtoLists {
    private ProtoLists() {
    }

    public static Int32ListPB int32List(Collection<Integer> values) {
        return Int32ListPB.newBuilder().addAllValues(values).build();
    }

    public static List<Integer> toIntegers(Int32ListPB pb) {
        return new ArrayList<>(pb.getValuesList());
    }

    public static Int64ListPB int64List(Collection<Long> values) {
        return Int64ListPB.newBuilder().addAllValues(values).build();
    }

    public static List<Long> toLongs(Int64ListPB pb) {
        return new ArrayList<>(pb.getValuesList());
    }

    public static StringListPB stringList(Collection<String> values) {
        return StringListPB.newBuilder().addAllValues(values).build();
    }

    public static List<String> toStrings(StringListPB pb) {
        return new ArrayList<>(pb.getValuesList());
    }
}
