// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.thrift.TTypeDesc;

/**
 * A pseudo-type cannot be used as a column data type, but it can be used to declare a function's argument or
 * result type. Each of the available pseudo-types is useful in situations where a function's behavior does not
 * correspond to simply taking or returning a value of a specific SQL data type.
 * <p>
 * Reference: https://www.postgresql.org/docs/12/datatype-pseudo.html
 */
public class PseudoType extends Type {
    public static final PseudoType ANY_ELEMENT = new AnyElementType();
    public static final PseudoType ANY_ARRAY = new AnyArrayType();
    public static final PseudoType ANY_MAP = new AnyMapType();
    public static final PseudoType ANY_STRUCT = new AnyStructType();

    @Override
    protected String prettyPrint(int lpad) {
        return null;
    }

    @Override
    public void toThrift(TTypeDesc container) {
        Preconditions.checkArgument(false, "PseudoType should not exposed to external");
    }

    @Override
    protected String toSql(int depth) {
        return toString();
    }
}
