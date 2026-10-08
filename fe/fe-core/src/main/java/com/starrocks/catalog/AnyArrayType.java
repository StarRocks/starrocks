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


package com.starrocks.catalog;

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/catalog/AnyArrayType.java
public class AnyArrayType extends PseudoType {
    @Override
    public boolean equals(Object t) {
        return t instanceof AnyArrayType;
=======
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class LanceTable extends Table {

    @SerializedName(value = "uri")
    private final String uri;

    // The Lance catalog this table belongs to. Without it the table reported the internal catalog's name, so it
    // counted as a meta lock target although it never lives in an internal database (see isMetaLockTarget).
    private final String catalogName;

    public LanceTable(long id, String catalogName, String name, List<Column> schema, String uri) {
        super(id, name, TableType.LANCE, schema);
        this.catalogName = catalogName;
        this.uri = uri;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getUri() {
        return uri;
>>>>>>> bf42f2c ([BugFix] Keep MYSQL, JDBC, ES, ExternalOlapTable and per-statement tables off the FE planning lock (#80240)):fe/fe-core/src/main/java/com/starrocks/catalog/LanceTable.java
    }

    @Override
    public boolean matchesType(Type t) {
        return t instanceof AnyArrayType || t instanceof AnyElementType || t.isArrayType();
    }

    @Override
    public String toString() {
        return "PseudoType.AnyArrayType";
    }
}
