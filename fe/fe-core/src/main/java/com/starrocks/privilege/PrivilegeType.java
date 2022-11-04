// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import java.util.HashMap;
import java.util.Map;

public enum PrivilegeType {
    TABLE(1, TableAction.actionMap(), "TABLES"),
    DATABASE(2, DbAction.actionMap(), "DATABASES"),
    SYSTEM(3, SystemAction.actionMap(), null),
    USER(4, UserAction.actionMap(), "USERS");

    private final int id;
    private final Map<String, Action> actionMap;

    // used in ALL statement, e.g. grant select on all tables in all databases
    private final String plural;
    PrivilegeType(int id, Map<String, Action> actionMap, String plural) {
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
    public enum TableAction {
        DELETE(1),
        DROP(2),
        INSERT(3),
        SELECT(4),
        SHOW(5);

        private final int id;

        TableAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (TableAction action : TableAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum DbAction {
        CREATE_TABLE(1),
        DROP(2),
        SHOW(3);

        private final int id;

        DbAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (DbAction action : DbAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum SystemAction {
        GRANT(1),
        NODE(2);  // AND MORE...

        private final int id;

        SystemAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (SystemAction action : SystemAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum UserAction {
        IMPERSONATE(1);

        private final int id;

        UserAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (UserAction action : UserAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }
}