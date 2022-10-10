// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum PrivilegeTypes {
    TABLE(TableActions.values()),
    DATABASE(DbActions.values());

    private final List<String> validActions;
    PrivilegeTypes(Object[] validObject) {
        this.validActions = Stream.of(validObject).map(o -> o.toString()).collect(Collectors.toList());
    }

    public List<String> getValidActions() {
        return validActions;
    }

    /**
     * Below defines all validate actions of a certain type
     */
    public enum TableActions {
        SELECT,
        INSERT,
        DELETE
    }

    public enum DbActions {
        CREATE_TABLE,
        DROP
    }
}
