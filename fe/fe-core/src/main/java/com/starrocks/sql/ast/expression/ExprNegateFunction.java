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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

public final class ExprNegateFunction {

    private ExprNegateFunction() {
    }

    public static boolean isSupportNegate(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            BinaryType op = ((BinaryPredicate) expr).getOp();
            return op == BinaryType.EQ
                    || op == BinaryType.NE
                    || op == BinaryType.LT
                    || op == BinaryType.LE
                    || op == BinaryType.GE
                    || op == BinaryType.GT;
        } else {
            return expr instanceof CompoundPredicate
                    || expr instanceof MultiInPredicate
                    || expr instanceof InPredicate
                    || expr instanceof ExistsPredicate
                    || expr instanceof IsNullPredicate;
        }
    }

    public static Expr negate(Expr expr) {
        Preconditions.checkNotNull(expr, "expression cannot be null");
        if (expr instanceof BinaryPredicate) {
            return negateBinaryPredicate((BinaryPredicate) expr);
        } else if (expr instanceof CompoundPredicate) {
            return negateCompoundPredicate((CompoundPredicate) expr);
        } else if (expr instanceof LargeInPredicate) {
            return negateLargeInPredicate((LargeInPredicate) expr);
        } else if (expr instanceof MultiInPredicate) {
            return negateMultiInPredicate((MultiInPredicate) expr);
        } else if (expr instanceof InPredicate) {
            return negateInPredicate((InPredicate) expr);
        } else if (expr instanceof ExistsPredicate) {
            return negateExistsPredicate((ExistsPredicate) expr);
        } else if (expr instanceof IsNullPredicate) {
            return negateIsNullPredicate((IsNullPredicate) expr);
        }
        return defaultNegate(expr);
    }

    private static Expr negateBinaryPredicate(BinaryPredicate predicate) {
        BinaryType newOp;
        switch (predicate.getOp()) {
            case EQ:
                newOp = BinaryType.NE;
                break;
            case NE:
                newOp = BinaryType.EQ;
                break;
            case LT:
                newOp = BinaryType.GE;
                break;
            case LE:
                newOp = BinaryType.GT;
                break;
            case GE:
                newOp = BinaryType.LT;
                break;
            case GT:
                newOp = BinaryType.LE;
                break;
            default:
                throw new IllegalStateException("Not implemented");
        }
        return new BinaryPredicate(newOp, predicate.getChild(0), predicate.getChild(1), predicate.getPos());
    }

    private static Expr negateCompoundPredicate(CompoundPredicate predicate) {
        if (predicate.getOp() == CompoundPredicate.Operator.NOT) {
            return predicate.getChild(0);
        }
        Expr negatedLeft = negate(predicate.getChild(0));
        Expr negatedRight = negate(predicate.getChild(1));
        CompoundPredicate.Operator newOp =
                predicate.getOp() == CompoundPredicate.Operator.OR
                        ? CompoundPredicate.Operator.AND
                        : CompoundPredicate.Operator.OR;
        return new CompoundPredicate(newOp, negatedLeft, negatedRight, predicate.getPos());
    }

    private static Expr negateInPredicate(InPredicate predicate) {
        List<Expr> inList = Lists.newArrayList(predicate.getListChildren());
        return new InPredicate(predicate.getChild(0), inList, !predicate.isNotIn(), predicate.getPos());
    }

    private static Expr negateLargeInPredicate(LargeInPredicate predicate) {
        List<Expr> inList = Lists.newArrayList(predicate.getListChildren());
        return new LargeInPredicate(predicate.getChild(0), predicate.getRawText(),
                predicate.getRawConstantList(), predicate.getConstantCount(), !predicate.isNotIn(),
                inList, predicate.getPos());
    }

    private static Expr negateMultiInPredicate(MultiInPredicate predicate) {
        List<Expr> outerExprs = Lists.newArrayList();
        for (int i = 0; i < predicate.getNumberOfColumns(); i++) {
            outerExprs.add(predicate.getChild(i));
        }
        Expr subquery = predicate.getChild(predicate.getNumberOfColumns());
        return new MultiInPredicate(outerExprs, subquery, !predicate.isNotIn(), predicate.getPos());
    }

    private static Expr negateExistsPredicate(ExistsPredicate predicate) {
        return new ExistsPredicate((Subquery) predicate.getChild(0), !predicate.isNotExists(), predicate.getPos());
    }

    private static Expr negateIsNullPredicate(IsNullPredicate predicate) {
        return new IsNullPredicate(predicate.getChild(0), !predicate.isNotNull(), predicate.getPos());
    }

    private static Expr defaultNegate(Expr expr) {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, expr, null);
    }
}
