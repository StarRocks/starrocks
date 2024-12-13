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

package com.starrocks.connector;

import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

<<<<<<< HEAD
=======
import java.util.StringJoiner;

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
public class MetaPreparationItem {
    private final Table table;
    private final ScalarOperator predicate;
    private final long limit;
<<<<<<< HEAD

    public MetaPreparationItem(Table table, ScalarOperator predicate, long limit) {
        this.table = table;
        this.predicate = predicate;
        this.limit = limit;
=======
    private final TableVersionRange version;

    public MetaPreparationItem(Table table, ScalarOperator predicate, long limit, TableVersionRange version) {
        this.table = table;
        this.predicate = predicate;
        this.limit = limit;
        this.version = version;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public Table getTable() {
        return table;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public long getLimit() {
        return limit;
    }

<<<<<<< HEAD
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MetaPreparationItem{");
        sb.append("table=").append(table);
        sb.append(", predicate=").append(predicate);
        sb.append(", limit=").append(limit);
        sb.append('}');
        return sb.toString();
=======
    public TableVersionRange getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MetaPreparationItem.class.getSimpleName() + "[", "]")
                .add("table=" + table)
                .add("predicate=" + predicate)
                .add("limit=" + limit)
                .add("version=" + version)
                .toString();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
