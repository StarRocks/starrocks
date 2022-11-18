// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import java.util.HashMap;
import java.util.Map;

public enum PrivilegeType {
    TABLE(1, TableAction.actionMap(), "TABLES"),
    DATABASE(2, DbAction.actionMap(), "DATABASES"),
    SYSTEM(3, SystemAction.actionMap(), null),
    USER(4, UserAction.actionMap(), "USERS"),
    RESOURCE(5, ResourceAction.actionMap(), "RESOURCES"),
    VIEW(6, ViewAction.actionMap(), "VIEWS"),
    CATALOG(7, CatalogAction.actionMap(), "CATALOGS");

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
        ALTER(5),
        EXPORT(6);

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
        ALTER(3),
        CREATE_VIEW(4),
        CREATE_FUNCTION(5),
        CREATE_MATERIALIZED_VIEW(6);

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
        NODE(2),
        CREATE_RESOURCE(3),
        PLUGIN(4),
        FILE(5),
        BLACKLIST(6),
        OPERATE(7),
        CREATE_EXTERNAL_CATALOG(8);  // AND MORE...

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

    public enum ResourceAction {
        USAGE(1),
        ALTER(2),
        DROP(3);

        private final int id;

        ResourceAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (ResourceAction action : ResourceAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum CatalogAction {
        USAGE(1),
        CREATE_DATABASE(2),
        DROP(3),
        ALTER(4);
        private final int id;

        CatalogAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (CatalogAction action : CatalogAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }

    public enum ViewAction {
        SELECT(1),
        ALTER(2),
        DROP(3);

        private final int id;

        ViewAction(int id) {
            this.id = id;
        }

        public static Map<String, Action> actionMap() {
            Map<String, Action> ret = new HashMap<>();
            for (ViewAction action : ViewAction.values()) {
                ret.put(action.toString(), new Action((short) action.id, action.toString()));
            }
            return ret;
        }
    }
}
