// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.analysis;

import com.amazonaws.thirdparty.joda.time.DateTime;
import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;

import java.util.List;

// this class is use to help same default value for every column consistent
public class DefaultValueResolver {

    private final String INIT_TIME = DateTime.now().toString();

    public static DefaultValueResolver build() {
        return new DefaultValueResolver();
    }

    // if the column have a default value or default expr can be calculated like now(). return true
    // else for a batch of every row different like uuid(). return false
    public static boolean hasDefaultValue(Column column) {
        if (column.getDefaultValue() != null) {
            return true;
        } else if (column.getDefaultExpr() != null) {
            if("now()".equalsIgnoreCase(column.getDefaultExpr().getExpr())) {
                return true;
            }
        }
        return false;
    }

    // if the column have a default value or default expr can be calculated like now(). return calculated value
    // else for a batch of every row different like uuid(). return null
    // consistency requires upper-level assurance
    public String getCalculatedDefaultValue(Column column) {
        if (column.getDefaultValue() != null) {
            return column.getDefaultValue();
        }
        if("now()".equalsIgnoreCase(column.getDefaultExpr().getExpr())) {
            // current transaction time
            if (ConnectContext.get() != null) {
                return ConnectContext.get().getTransactionStartTime().toString();
            } else {
                // not available for asynchronous calls for example:
                // alter table xx add column t0 datetime not null default CURRENT_TIMESTAMP;
                return INIT_TIME;
            }
        }
        return null;
    }

    public static String getMetaDefaultValue(Column column, List<String> extras) {
        if (column.getDefaultValue() != null) {
            return column.getDefaultValue();
        } else if (column.getDefaultExpr() != null) {
            if ("now()".equalsIgnoreCase(column.getDefaultExpr().getExpr())) {
                extras.add("DEFAULT_GENERATED");
                return "CURRENT_TIMESTAMP";
            }
        }
        return FeConstants.null_string;
    }

}
