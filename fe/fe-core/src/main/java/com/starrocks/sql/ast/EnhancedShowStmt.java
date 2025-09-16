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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Enhanced SHOW statement that supports advanced query features.
 * <p>
 * This abstract class extends the basic ShowStmt to provide enhanced functionality
 * for SHOW statements, including:
 * <p>
 * 1. **Predicate Filtering**: Support for WHERE clauses to filter results
 * - Only supports equality (=) and LIKE operators for security and performance
 * - Allows filtering of SHOW statement results based on column values
 * <p>
 * 2. **Sorting**: Support for ORDER BY clauses to sort results
 * - Multiple sort columns supported
 * - Both ascending and descending order supported
 * <p>
 * 3. **Result Limiting**: Support for LIMIT clauses to restrict result size
 * - Useful for large result sets to improve performance
 * - Supports both LIMIT count and LIMIT offset, count syntax
 * <p>
 * 4. **Query Transformation**: Can be converted to equivalent SELECT statements
 * - When WHERE clause is present, the SHOW statement can be transformed
 * - into a SELECT statement for more efficient execution
 * <p>
 * Examples of enhanced SHOW statements:
 * - SHOW TABLES WHERE Table_type = 'BASE TABLE' ORDER BY Table_name LIMIT 10
 * - SHOW COLUMNS FROM table_name WHERE Field LIKE 'user_%' ORDER BY Field
 * - SHOW DATABASES WHERE Database_name != 'information_schema' LIMIT 5
 * <p>
 * This class is used by various SHOW statement implementations such as:
 * - ShowTableStmt: SHOW TABLES with filtering and sorting
 * - ShowColumnStmt: SHOW COLUMNS with filtering and sorting
 * - ShowDbStmt: SHOW DATABASES with filtering and sorting
 * - And many other SHOW statement types
 *
 * @see ShowStmt Base class for all SHOW statements
 * @see ShowTableStmt Example implementation for SHOW TABLES
 * @see ShowColumnStmt Example implementation for SHOW COLUMNS
 */
public abstract class EnhancedShowStmt extends ShowStmt {
    /**
     * WHERE clause predicate for filtering results. Only supports = and LIKE operators.
     */
    protected Predicate predicate;

    /**
     * LIMIT clause element for restricting result size (e.g., LIMIT 10 or LIMIT 5, 10).
     */
    protected LimitElement limitElement;

    /**
     * ORDER BY elements parsed from SQL (e.g., ORDER BY col1 ASC, col2 DESC).
     */
    protected List<OrderByElement> orderByElements;

    /**
     * Processed ORDER BY pairs used for execution.
     */
    protected List<OrderByPair> orderByPairs;

    protected EnhancedShowStmt(NodePosition pos) {
        super(pos);
    }

    /**
     * Sets the WHERE clause predicate for filtering results.
     *
     * @param predicate The predicate expression (only = and LIKE operators supported)
     */
    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    /**
     * Gets the WHERE clause predicate.
     *
     * @return The predicate expression, or null if no WHERE clause
     */
    public Predicate getPredicate() {
        return predicate;
    }

    /**
     * Converts this enhanced SHOW statement to an equivalent SELECT statement.
     * This is used when a WHERE clause is present to enable more efficient execution.
     *
     * @return A QueryStatement equivalent to this SHOW statement, or null if no conversion is needed
     * @throws AnalysisException If the conversion fails
     */
    public QueryStatement toSelectStmt() throws AnalysisException {
        return null;
    }

    /**
     * Gets the ORDER BY elements parsed from the SQL statement.
     *
     * @return List of ORDER BY elements, or null if no ORDER BY clause
     */
    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    /**
     * Gets the processed ORDER BY pairs used for execution.
     *
     * @return List of ORDER BY pairs, or null if no ORDER BY clause
     */
    public List<OrderByPair> getOrderByPairs() {
        return orderByPairs;
    }

    /**
     * Sets the processed ORDER BY pairs for execution.
     *
     * @param orderByPairs The processed ORDER BY pairs
     */
    public void setOrderByPairs(List<OrderByPair> orderByPairs) {
        this.orderByPairs = orderByPairs;
    }

    /**
     * Gets the LIMIT clause element.
     *
     * @return The LIMIT element, or null if no LIMIT clause
     */
    public LimitElement getLimitElement() {
        return limitElement;
    }
}
