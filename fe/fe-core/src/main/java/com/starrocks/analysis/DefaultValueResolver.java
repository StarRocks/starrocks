// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.common.FeConstants;
import org.joda.time.DateTime;

import java.util.List;

// this class is use to help same default value for every column consistent
public class DefaultValueResolver {

    private final String INIT_TIME = DateTime.now().toString();

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

    // only for check use, no guarantee of consistency
    public static String getCalculatedDefaultValueForCheck(Column column) {
        if (column.getDefaultValue() != null) {
            return column.getDefaultValue();
        }
        if("now()".equalsIgnoreCase(column.getDefaultExpr().getExpr())) {
            return DateTime.now().toString();
        }
        return null;
    }

    // if the column have a default value or default expr can be calculated like now(). return calculated value
    // else for a batch of every row different like uuid(). return null
    // consistency requires upper-level assurance
    public String getCalculatedDefaultValue(Column column) {
        if (column.getDefaultValue() != null) {
            return column.getDefaultValue();
        }
        if("now()".equalsIgnoreCase(column.getDefaultExpr().getExpr())) {
            return INIT_TIME;
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
