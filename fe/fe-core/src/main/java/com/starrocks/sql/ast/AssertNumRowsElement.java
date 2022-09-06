// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.thrift.TAssertion;

public class AssertNumRowsElement {
    public enum Assertion {
        EQ, // val1 == val2
        NE, // val1 != val2
        LT, // val1 < val2
        LE, // val1 <= val2
        GT, // val1 > val2
        GE; // val1 >= val2

        public TAssertion toThrift() {
            switch (this) {
                case EQ:
                    return TAssertion.EQ;
                case NE:
                    return TAssertion.NE;
                case LT:
                    return TAssertion.LT;
                case LE:
                    return TAssertion.LE;
                case GT:
                    return TAssertion.GT;
                case GE:
                    return TAssertion.GE;
                default:
                    return null;
            }
        }
    }

    private final long desiredNumOfRows;
    private final String subqueryString;
    private final Assertion assertion;

    public AssertNumRowsElement(long desiredNumOfRows, String subqueryString, Assertion assertion) {
        this.desiredNumOfRows = desiredNumOfRows;
        this.subqueryString = subqueryString;
        this.assertion = assertion;
    }

    public long getDesiredNumOfRows() {
        return desiredNumOfRows;
    }

    public String getSubqueryString() {
        return subqueryString;
    }

    public Assertion getAssertion() {
        return assertion;
    }
}
