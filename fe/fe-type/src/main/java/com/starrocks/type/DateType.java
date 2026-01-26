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

package com.starrocks.type;

import com.google.common.collect.ImmutableList;

public class DateType extends ScalarType {
    public static final DateType DATE = new DateType(PrimitiveType.DATE);
    public static final DateType DATETIME = new DateType(PrimitiveType.DATETIME);
    public static final DateType TIME = new DateType(PrimitiveType.TIME);

    public static final ImmutableList<ScalarType> DATE_TYPES = ImmutableList.of(DATE, DATETIME);

    public DateType(PrimitiveType primitiveType) {
        super(primitiveType);
    }
}
