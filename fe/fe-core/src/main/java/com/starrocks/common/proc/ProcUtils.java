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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.server.GlobalStateMgr;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProcUtils {

    /**
     * Apply a SHOW statement's WHERE, ORDER BY and LIMIT to rows a proc dir has already fetched.
     *
     * Everything after "fetch the rows" is the same for every layout, and was copied out three
     * times to prove it: SchemaChangeProcDir, OptimizeProcDir and RollupProcDir differed only in
     * the call that produced `rows`, down to OptimizeProcDir still naming its local variable
     * schemaChangeJobInfos. What is left in each proc dir is the part that is actually its own.
     *
     * A row whose width does not match the title list fails the statement with an AnalysisException,
     * checked up front rather than as the filter walks the two lists by index. See checkRowWidths.
     *
     * @throws AnalysisException if any row's width does not match titleNames
     */
    public static ProcResult applyFilterOrderLimit(List<String> titleNames, List<List<Comparable>> rows,
                                                   Map<String, Expr> filter, List<OrderByPair> orderByPairs,
                                                   LimitElement limitElement) throws AnalysisException {
        // Before the filter, not only inside it: the loop below pairs titleNames.get(i) with
        // row.get(i), so a mismatched width would compare against the wrong column or run off the
        // end of one of the two lists.
        checkRowWidths(titleNames, rows);

        List<List<Comparable>> selected;
        if (filter == null || filter.isEmpty()) {
            selected = rows;
        } else {
            selected = Lists.newArrayList();
            for (List<Comparable> row : rows) {
                boolean isNeed = true;
                for (int i = 0; i < row.size(); i++) {
                    isNeed = filterResult(titleNames.get(i), row.get(i), filter);
                    if (!isNeed) {
                        break;
                    }
                }
                if (isNeed) {
                    selected.add(row);
                }
            }
        }

        if (orderByPairs != null && !orderByPairs.isEmpty()) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            Collections.sort(selected, new ListComparator<>(orderByPairs.toArray(orderByPairArr)));
        }

        if (limitElement != null && limitElement.hasLimit()) {
            int beginIndex = (int) limitElement.getOffset();
            int endIndex = (int) (beginIndex + limitElement.getLimit());
            if (endIndex > selected.size()) {
                endIndex = selected.size();
            }
            selected = selected.subList(beginIndex, endIndex);
        }

        return toProcResult(titleNames, selected);
    }

    /**
     * Refuse rows that do not match the title list.
     *
     * toProcResult calls this, which covers every answer. A caller that walks the two lists together
     * - a WHERE filter does - needs it before that walk as well, since by then a mismatched row has
     * already been compared against the wrong columns or run off the end of one of them.
     *
     * @throws AnalysisException if any row's width does not match titleNames
     */
    public static void checkRowWidths(List<String> titleNames, List<List<Comparable>> rows)
            throws AnalysisException {
        for (List<Comparable> row : rows) {
            if (row.size() != titleNames.size()) {
                throw new AnalysisException("row has " + row.size() + " columns but "
                        + titleNames.size() + " were expected: " + titleNames);
            }
        }
    }

    /**
     * Build a ProcResult from rows, under the given column titles.
     *
     * Used by the proc dirs behind SHOW ALTER TABLE and SHOW PARTITIONS - SchemaChangeProcDir,
     * OptimizeProcDir, RollupProcDir and PartitionsProcDir. Each reaches an answer by two roads:
     * fetchResult builds one directly, and fetchResultByFilter builds one after applying the
     * statement's WHERE, ORDER BY and LIMIT. Those eight entry points delegate here.
     *
     * Most other proc dirs still build their own. Some are the same loop and could move here; some,
     * like LoadProcDir, interleave it with merging and reversing their rows. Nothing about this
     * helper requires them to, and it does not claim to cover them.
     *
     * Because those eight roads end here, this is where their rows are checked against the titles
     * they are about to be sent under. sendShowResult does not check: it declares the title list as
     * the column set and then writes however many cells each row happens to have, so a row missing
     * a column in the middle goes out with every value after it shifted one column left, under
     * headers that no longer describe them. Such a row fails the statement instead.
     *
     * @throws AnalysisException if any row's width does not match titleNames
     */
    public static BaseProcResult toProcResult(List<String> titleNames, List<List<Comparable>> rows)
            throws AnalysisException {
        checkRowWidths(titleNames, rows);

        BaseProcResult result = new BaseProcResult();
        result.setNames(titleNames);
        for (List<Comparable> row : rows) {
            List<String> oneResult = new ArrayList<>(row.size());
            for (Comparable column : row) {
                oneResult.add(column.toString());
            }
            result.addRow(oneResult);
        }
        return result;
    }

    /**
     * Decide whether one cell of one row survives a SHOW statement's WHERE clause.
     *
     * The filter map is keyed by lower-cased column name and was built by the statement's analyzer,
     * which is also where a date column's right-hand side was cast to DATETIME. That cast is what
     * this reads: a right side that is still a StringLiteral can only be compared for equality,
     * while a DateLiteral gets the full set of operators. A column the filter does not mention, or
     * a shape neither branch recognises, passes - filtering is opt-in per column.
     */
    public static boolean filterResult(String columnName, Comparable<?> element, Map<String, Expr> filter)
            throws AnalysisException {
        if (filter == null) {
            return true;
        }
        Expr subExpr = filter.get(columnName.toLowerCase());
        if (subExpr == null) {
            return true;
        }
        BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
        if (subExpr.getChild(1) instanceof StringLiteral && binaryPredicate.getOp() == BinaryType.EQ) {
            return ((StringLiteral) subExpr.getChild(1)).getValue().equals(element);
        }
        if (subExpr.getChild(1) instanceof DateLiteral) {
            LocalDateTime elementDateTime = DateUtils.parseStrictDateTime(element.toString());
            Long leftVal = new DateLiteral(elementDateTime, Type.DATETIME).getLongValue();
            Long rightVal = ((DateLiteral) subExpr.getChild(1)).getLongValue();
            switch (binaryPredicate.getOp()) {
                case EQ:
                case EQ_FOR_NULL:
                    return leftVal.equals(rightVal);
                case GE:
                    return leftVal >= rightVal;
                case GT:
                    return leftVal > rightVal;
                case LE:
                    return leftVal <= rightVal;
                case LT:
                    return leftVal < rightVal;
                case NE:
                    return !leftVal.equals(rightVal);
                default:
                    Preconditions.checkState(false, "No defined binary operator.");
            }
        }
        return true;
    }

    /**
     * Resolve a column name against a proc dir's title list, for the ORDER BY of a SHOW statement.
     *
     * Every proc dir that accepts ORDER BY needs this, and each one used to carry its own copy -
     * eight of them, in three spellings that differed only in how they got at the index. They all
     * answer the same question, and the answer must not drift: the index is handed straight to
     * ListComparator, so resolving against the wrong list, or resolving differently, sorts by
     * whatever column happens to sit at that position.
     *
     * The list is passed in rather than read from a field because it is per statement: the layouts
     * disagree on both names and positions, and PartitionsProcDir even builds its list at runtime.
     */
    public static int analyzeColumn(List<String> titleNames, String columnName) throws AnalysisException {
        for (int i = 0; i < titleNames.size(); ++i) {
            if (titleNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    static long getDbId(String dbIdOrName) throws AnalysisException {
        try {
            return Long.parseLong(dbIdOrName);
        } catch (NumberFormatException e) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbIdOrName);
            if (db == null) {
                throw new AnalysisException("Unknown database id or name \"" + dbIdOrName + "\"");
            }
            return db.getId();
        }
    }

}
