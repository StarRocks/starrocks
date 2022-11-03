// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import java.util.HashMap;
import java.util.Map;

public enum PrivilegeTypes {
    TABLE(1, TableActions.actionMap(), "TABLES"),
    DATABASE(2, DbActions.actionMap(), "DATABASES"),
    SYSTEM(3, SystemActions.actionMap(), null),
    USER(4, UserActions.actionMap(), "USERS");

    private final int id;
    private final Map<String, Action> actionMap;

    // used in ALL statement, e.g. grant select on all tables in all databases
    private final String plural;
    PrivilegeTypes(int id, Map<String, Action> actionMap, String plural) {
        this.id = id;
        this.actionMap = actionMap;
        this.plural = plural;
    }

    public Map<String, Action> getActionMap() {
        return actionMap;
    }

    public String getPlural() {
        return plural;
    }

    public int getId() {
        return id;
    }

    /**
     * Below defines all validate actions of a certain type
     */
    public enum TableActions {
        DELETE(1),
        DROP(2),
        INSERT(3),
        SELECT(4),
        SHOW(5);

        private final int id;

        TableActions(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (TableActions action : TableActions.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum DbActions {
        CREATE_TABLE(1),
        DROP(2),
        SHOW(3);

        private final int id;

        DbActions(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (DbActions action : DbActions.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum SystemActions {
        GRANT(1),
        NODE(2);  // AND MORE...

        private final int id;

        SystemActions(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (SystemActions action : SystemActions.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum UserActions {
        IMPERSONATE(1);

        private final int id;

        UserActions(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (UserActions action : UserActions.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }
}