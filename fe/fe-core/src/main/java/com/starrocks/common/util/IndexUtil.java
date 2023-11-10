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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/TimeUtils.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.starrocks.analysis.InvertedIndexUtil;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Locale;
import java.util.Map;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.CLUCENE;

public class IndexUtil {

    public static void checkInvertedIndexValid(Column column, Map<String, String> properties, KeysType keysType) {
        if (keysType != KeysType.DUP_KEYS) {
            throw new SemanticException("The inverted index can only be build on DUPLICATE table.");
        }
        if (!(column.getType() instanceof ScalarType)) {
            throw new SemanticException("The inverted index can only be build on column with type of scalar type.");
        }

        String impLibKey = IMP_LIB.name().toLowerCase(Locale.ROOT);
        if (properties.containsKey(impLibKey)) {
            String impValue = properties.get(impLibKey);
            if (!CLUCENE.name().equalsIgnoreCase(impValue)) {
                throw new SemanticException("Only support clucene implement for now");
            }
        }

        InvertedIndexUtil.checkInvertedIndexParser(column.getName(), column.getPrimitiveType(), properties);
    }
}
