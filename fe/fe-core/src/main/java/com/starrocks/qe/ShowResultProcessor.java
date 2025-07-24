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

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.ShowStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class to process show command results with WHERE, ORDER BY, LIKE, and LIMIT clauses
 */
public class ShowResultProcessor {
    private static final Logger LOG = LogManager.getLogger(ShowResultProcessor.class);

    /**
     * Process show command results by applying WHERE, ORDER BY, LIKE, and LIMIT clauses
     */
    public static ShowResultSet processShowResult(ShowStmt statement, ShowResultSetMetaData metaData, 
                                                 List<List<String>> rawRows) {
        try {
            List<List<String>> processedRows = rawRows;
            
            // Apply LIKE pattern filtering
            if (!Strings.isNullOrEmpty(statement.getPattern())) {
                processedRows = applyLikeFilter(processedRows, metaData, statement.getPattern());
            }
            
            // Apply WHERE clause filtering
            if (statement.getPredicate() != null) {
                processedRows = applyWhereFilter(processedRows, metaData, statement.getPredicate());
            }
            
            // Apply ORDER BY sorting
            if (statement.getOrderByElements() != null && !statement.getOrderByElements().isEmpty()) {
                processedRows = applySorting(processedRows, metaData, statement.getOrderByElements());
            }
            
            // Apply LIMIT clause
            if (statement.getLimitElement() != null) {
                processedRows = applyLimit(processedRows, statement.getLimitElement());
            }
            
            return new ShowResultSet(metaData, processedRows);
        } catch (Exception e) {
            LOG.warn("Error processing show result", e);
            // Return original result if processing fails
            return new ShowResultSet(metaData, rawRows);
        }
    }

    /**
     * Apply LIKE pattern filtering to the first column (typically name column)
     */
    private static List<List<String>> applyLikeFilter(List<List<String>> rows, ShowResultSetMetaData metaData, 
                                                     String pattern) {
        if (rows.isEmpty() || metaData.getColumns().isEmpty()) {
            return rows;
        }
        
        // Convert SQL LIKE pattern to regex
        String regexPattern = pattern.replace("%", ".*").replace("_", ".");
        Pattern compiledPattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
        
        return rows.stream()
                .filter(row -> !row.isEmpty() && compiledPattern.matcher(row.get(0)).matches())
                .collect(Collectors.toList());
    }

    /**
     * Apply WHERE clause filtering
     */
    private static List<List<String>> applyWhereFilter(List<List<String>> rows, ShowResultSetMetaData metaData, 
                                                      Expr predicate) {
        if (rows.isEmpty() || metaData.getColumns().isEmpty()) {
            return rows;
        }
        
        // Create column name to index mapping
        Map<String, Integer> columnIndexMap = createColumnIndexMap(metaData);
        
        return rows.stream()
                .filter(row -> evaluatePredicate(row, columnIndexMap, predicate))
                .collect(Collectors.toList());
    }

    /**
     * Apply ORDER BY sorting
     */
    private static List<List<String>> applySorting(List<List<String>> rows, ShowResultSetMetaData metaData, 
                                                  List<OrderByElement> orderByElements) {
        if (rows.isEmpty() || metaData.getColumns().isEmpty()) {
            return rows;
        }
        
        Map<String, Integer> columnIndexMap = createColumnIndexMap(metaData);
        
        Comparator<List<String>> comparator = null;
        
        for (OrderByElement orderByElement : orderByElements) {
            if (orderByElement.getExpr() instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                String columnName = slotRef.getColumnName();
                Integer columnIndex = columnIndexMap.get(columnName.toLowerCase());
                
                if (columnIndex != null) {
                    Comparator<List<String>> columnComparator = (row1, row2) -> {
                        String val1 = columnIndex < row1.size() ? row1.get(columnIndex) : "";
                        String val2 = columnIndex < row2.size() ? row2.get(columnIndex) : "";
                        
                        // Try numeric comparison first
                        try {
                            Long num1 = Long.parseLong(val1);
                            Long num2 = Long.parseLong(val2);
                            return num1.compareTo(num2);
                        } catch (NumberFormatException e) {
                            // Fall back to string comparison
                            return val1.compareToIgnoreCase(val2);
                        }
                    };
                    
                    if (!orderByElement.getIsAsc()) {
                        columnComparator = columnComparator.reversed();
                    }
                    
                    comparator = (comparator == null) ? columnComparator : comparator.thenComparing(columnComparator);
                }
            }
        }
        
        if (comparator != null) {
            return rows.stream().sorted(comparator).collect(Collectors.toList());
        }
        
        return rows;
    }

    /**
     * Apply LIMIT clause
     */
    private static List<List<String>> applyLimit(List<List<String>> rows, LimitElement limitElement) {
        if (rows.isEmpty()) {
            return rows;
        }
        
        long offset = limitElement.hasOffset() ? limitElement.getOffset() : 0;
        long limit = limitElement.hasLimit() ? limitElement.getLimit() : Long.MAX_VALUE;
        
        return rows.stream()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Create mapping from column name to column index
     */
    private static Map<String, Integer> createColumnIndexMap(ShowResultSetMetaData metaData) {
        List<Column> columns = metaData.getColumns();
        return columns.stream()
                .collect(Collectors.toMap(
                        column -> column.getName().toLowerCase(),
                        columns::indexOf
                ));
    }

    /**
     * Evaluate a predicate against a row
     */
    private static boolean evaluatePredicate(List<String> row, Map<String, Integer> columnIndexMap, Expr predicate) {
        try {
            if (predicate instanceof BinaryPredicate) {
                return evaluateBinaryPredicate(row, columnIndexMap, (BinaryPredicate) predicate);
            } else if (predicate instanceof LikePredicate) {
                return evaluateLikePredicate(row, columnIndexMap, (LikePredicate) predicate);
            } else if (predicate instanceof CompoundPredicate) {
                return evaluateCompoundPredicate(row, columnIndexMap, (CompoundPredicate) predicate);
            }
        } catch (Exception e) {
            LOG.warn("Error evaluating predicate: " + predicate, e);
        }
        
        // If we can't evaluate the predicate, include the row
        return true;
    }

    private static boolean evaluateBinaryPredicate(List<String> row, Map<String, Integer> columnIndexMap, 
                                                  BinaryPredicate predicate) {
        if (!(predicate.getChild(0) instanceof SlotRef) || !(predicate.getChild(1) instanceof StringLiteral)) {
            return true;
        }
        
        SlotRef slotRef = (SlotRef) predicate.getChild(0);
        StringLiteral literal = (StringLiteral) predicate.getChild(1);
        
        String columnName = slotRef.getColumnName().toLowerCase();
        Integer columnIndex = columnIndexMap.get(columnName);
        
        if (columnIndex == null || columnIndex >= row.size()) {
            return true;
        }
        
        String rowValue = row.get(columnIndex);
        String literalValue = literal.getStringValue();
        
        switch (predicate.getOp()) {
            case EQ:
                return rowValue.equalsIgnoreCase(literalValue);
            case NE:
                return !rowValue.equalsIgnoreCase(literalValue);
            case LT:
                return compareValues(rowValue, literalValue) < 0;
            case LE:
                return compareValues(rowValue, literalValue) <= 0;
            case GT:
                return compareValues(rowValue, literalValue) > 0;
            case GE:
                return compareValues(rowValue, literalValue) >= 0;
            default:
                return true;
        }
    }

    private static boolean evaluateLikePredicate(List<String> row, Map<String, Integer> columnIndexMap, 
                                                LikePredicate predicate) {
        if (!(predicate.getChild(0) instanceof SlotRef) || !(predicate.getChild(1) instanceof StringLiteral)) {
            return true;
        }
        
        SlotRef slotRef = (SlotRef) predicate.getChild(0);
        StringLiteral literal = (StringLiteral) predicate.getChild(1);
        
        String columnName = slotRef.getColumnName().toLowerCase();
        Integer columnIndex = columnIndexMap.get(columnName);
        
        if (columnIndex == null || columnIndex >= row.size()) {
            return true;
        }
        
        String rowValue = row.get(columnIndex);
        String pattern = literal.getStringValue();
        
        // Convert SQL LIKE pattern to regex
        String regexPattern = pattern.replace("%", ".*").replace("_", ".");
        Pattern compiledPattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
        
        boolean matches = compiledPattern.matcher(rowValue).matches();
        return predicate.getOp() == LikePredicate.Operator.LIKE ? matches : !matches;
    }

    private static boolean evaluateCompoundPredicate(List<String> row, Map<String, Integer> columnIndexMap, 
                                                    CompoundPredicate predicate) {
        boolean leftResult = evaluatePredicate(row, columnIndexMap, predicate.getChild(0));
        boolean rightResult = evaluatePredicate(row, columnIndexMap, predicate.getChild(1));
        
        switch (predicate.getOp()) {
            case AND:
                return leftResult && rightResult;
            case OR:
                return leftResult || rightResult;
            case NOT:
                return !leftResult;
            default:
                return true;
        }
    }

    private static int compareValues(String val1, String val2) {
        // Try numeric comparison first
        try {
            Long num1 = Long.parseLong(val1);
            Long num2 = Long.parseLong(val2);
            return num1.compareTo(num2);
        } catch (NumberFormatException e) {
            // Fall back to string comparison
            return val1.compareToIgnoreCase(val2);
        }
    }
}