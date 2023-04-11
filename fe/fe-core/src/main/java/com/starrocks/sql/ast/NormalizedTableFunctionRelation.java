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

package com.starrocks.sql.ast;

import com.starrocks.analysis.JoinOperator;

/**
 * {@link NormalizedTableFunctionRelation} is a special {@link JoinRelation} converted from TABLE(table_function(...)).
 *
 * <p>
 * The main difference between {@link NormalizedTableFunctionRelation} and {@link TableFunctionRelation} is that
 * {@link TableFunctionRelation} can only appear on the right side of a lateral join, while
 * {@link NormalizedTableFunctionRelation} can appear anywhere a regular table can appear:
 * <code>
 * SELECT * FROM TABLE(my_table_function(...));
 * SELECT * FROM t0, TABLE(my_table_function(...));
 * SELECT * FROM t0 LEFT JOIN TABLE(my_table_function(...)) t1(x, y) ON ...;
 * SELECT * FROM TABLE(my_table_function(...)) t1(x, y) LEFT JOIN t0 ON ...;
 * </code>
 */
public class NormalizedTableFunctionRelation extends JoinRelation {
    public NormalizedTableFunctionRelation(TableFunctionRelation tableFunctionRelation) {
        super(JoinOperator.CROSS_JOIN, ValuesRelation.newDualRelation(), tableFunctionRelation, null, true);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitNormalizedTableFunction(this, context);
    }
}
