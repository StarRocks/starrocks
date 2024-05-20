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

package com.starrocks.sql.automv.pn;

public enum BuiltinKind implements ApplyKind {
    SET_OF("$setOf"),
    OPEN_RANGE_OF("$openRangeOf"), //()
    OPEN_CLOSED_RANGE_OF("$openClosedRangeOf"), //(]
    CLOSED_OPEN_RANGE_OF("$closedOpenRangeOf"), //[)
    CLOSED_RANGE_OF("$closedRangeOf"), //[]
    MODIFY("$modify"),
    IN("$in"),
    IN_RANGE("$inRange"),
    CASE("$case"),
    CAST("$cast"),
    AND("$and"),
    OR("$or"),
    EQ("$eq"),
    NULL_SAFE_EQ("$nullSafeEq"),
    NE("$ne"),
    GT("$gt"),
    GE("$ge"),
    LT("$lt"),
    LE("$le"),
    ARRAY("$array"),
    MAP("$map"),
    ARRAY_SLICE("$arraySlice"),
    COLLECTION_ELEMENT("$collectionElement"),
    SUBFIELD("$subfield"),
    LIKE("$like"),
    REGEXP("$regexp"),
    LAMBDA("$lambda"),
    LOCAL("$local"),
    DISTINCT("$distinct"),
    VARIABLE("$variable");
    private final String showName;

    BuiltinKind(String showName) {
        this.showName = showName;
    }

    @Override
    public String toString() {
        return this.showName;
    }
}
