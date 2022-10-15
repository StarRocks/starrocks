// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum PrivilegeTypes {
    TABLE(TableActions.values(), "TABLES"),
    DATABASE(DbActions.values(), "DATABASES"),
    SYSTEM(SystemActions.values(), null),
    USER(UserActions.values(), null);

    private final List<String> validActions;

    // used in ALL statement, e.g. grant select on all tables in all databases
    private final String plural;
    PrivilegeTypes(Object[] validObject, String plural) {
        this.validActions = Stream.of(validObject).map(o -> o.toString()).collect(Collectors.toList());
        this.plural = plural;
    }

    public List<String> getValidActions() {
        return validActions;
    }

    public String getPlural() {
        return plural;
    }

    /**
     * Below defines all validate actions of a certain type
     */
    public enum TableActions {
        INSERT,
        DELETE,
        SELECT,
        SHOW
    }

    public enum DbActions {
        CREATE_TABLE,
        DROP,
        SHOW
    }

    public enum SystemActions {
        ADMIN,
        GRANT  // AND MORE...
    }

    public enum UserActions {
        IMPERSONATE
    }
}