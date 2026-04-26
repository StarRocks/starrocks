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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.base.Preconditions;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

final class DecodeUtil {

    private DecodeUtil() {}

    static Type getDictifiedType(Type type) {
        if (type == null) {
            return null;
        }
        Preconditions.checkState(!type.isStructType());
        if (type.isStringType()) {
            return IntegerType.INT;
        }
        if (type.isStringArrayType()) {
            return ArrayType.ARRAY_INT;
        }
        return type;
    }
}