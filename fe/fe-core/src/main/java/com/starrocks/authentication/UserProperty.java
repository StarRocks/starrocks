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


package com.starrocks.authentication;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// UserProperty is a class that represents the properties that are identified.
public class UserProperty {
    private static final Logger LOG = LogManager.getLogger(UserProperty.class);

    // Because session variables does not include these two properties, we define them here.
    public static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";
    public static final String PROP_DATABASE = "database";
    // In order to keep consistent with database, we support user to set session.catalog = xxx or catalog = yyy
    public static final String PROP_CATALOG = SessionVariable.CATALOG;
    public static final String PROP_SESSION_PREFIX = "session.";

    public static final long MAX_CONN_DEFAULT_VALUE = 1024;
    public static final String CATALOG_DEFAULT_VALUE = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    public static final String DATABASE_DEFAULT_VALUE = "";

    // If the values is empty, we remove the key from the session variables.
    public static final String EMPTY_VALUE = "";

    @SerializedName(value = "m")
    private long maxConn = MAX_CONN_DEFAULT_VALUE;

    @SerializedName(value = "d")
    private String database = DATABASE_DEFAULT_VALUE;

    @SerializedName(value = "c")
    private String catalog = CATALOG_DEFAULT_VALUE;

    @SerializedName(value = "s")
    private Map<String, String> sessionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public void update(String userName, List<Pair<String, String>> properties) throws DdlException {
        AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        UserIdentity user = authenticationMgr.getUserIdentityByName(userName);
        update(user, properties);
    }

    // update the user properties
    // we should check the properties and throw exceptions if the properties are invalid
    public void update(UserIdentity user, List<Pair<String, String>> properties) throws DdlException {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        String newDatabase = getDatabase();
        String newCatalog = getCatalog();
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                checkMaxConn(value);
            } else if (key.equalsIgnoreCase(PROP_DATABASE)) {
                // we do not check database existence here, because we should
                // check catalog existence first.
                newDatabase = value;
            } else if (key.equalsIgnoreCase(PROP_CATALOG)) {
                checkCatalog(value);
                newCatalog = value;
            } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                if (sessionKey.equalsIgnoreCase(PROP_CATALOG)) {
                    checkCatalog(value);
                    newCatalog = value;
                } else {
                    checkSessionVariable(sessionKey, value);
                }
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }
        if (!newDatabase.equalsIgnoreCase(getDatabase())) {
            checkDatabase(newCatalog, newDatabase);
        }

        newDatabase = getDatabase();
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                long maxConn = checkMaxConn(value);
                setMaxConn(maxConn);
            } else if (key.equalsIgnoreCase(PROP_DATABASE)) {
                // we do not check database existence here, because we should
                // check catalog existence first.
                newDatabase = value;
            } else if (key.equalsIgnoreCase(PROP_CATALOG)) {
                setCatalog(value);
            } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                if (sessionKey.equalsIgnoreCase(PROP_CATALOG)) {
                    setCatalog(value);
                } else {
                    setSessionVariable(sessionKey, value);
                }
            }
        }
        if (!newDatabase.equalsIgnoreCase(getDatabase())) {
            setDatabase(newDatabase);
        }
    }

    // We do not check the variable default_session_database and default_session_catalog here, because we have checked them
    // when set properties. And we never should throw exceptions, this may cause the system can be started normally.
    public void updateForReplayJournal(List<Pair<String, String>> properties) {
        for (Pair<String, String> entry : properties) {
            try {
                String key = entry.first;
                String value = entry.second;
                if (key.equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                    long maxConn = checkMaxConn(value);
                    setMaxConn(maxConn);
                } else if (key.equalsIgnoreCase(PROP_DATABASE)) {
                    setDatabase(value);
                } else if (key.equalsIgnoreCase(PROP_CATALOG)) {
                    setCatalog(value);
                } else if (key.startsWith(PROP_SESSION_PREFIX)) {
                    String sessionKey = key.substring(PROP_SESSION_PREFIX.length());
                    if (sessionKey.equalsIgnoreCase(PROP_CATALOG)) {
                        setCatalog(value);
                    } else {
                        setSessionVariable(sessionKey, value);
                    }
                }
            } catch (Exception e) {
                // we should never throw an exception when replaying journal
                LOG.warn("update user property from journal failed: ", e);
            }
        }
    }


    public String getCatalogDbName() {
        return getCatalog() + "." + getDatabase();
    }

    public long getMaxConn() {
        return maxConn;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String sessionDatabase) {
        if (sessionDatabase.equalsIgnoreCase(EMPTY_VALUE)) {
            this.database = DATABASE_DEFAULT_VALUE;
        } else {
            this.database = sessionDatabase;
        }
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void setSessionVariables(Map<String, String> sessions) {
        this.sessionVariables = sessions;
    }

    // check the session variable
    private void checkSessionVariable(String sessionKey, String value) throws DdlException {
        if (value.equalsIgnoreCase(EMPTY_VALUE)) {
            return;
        }
        // check whether the variable exists
        SystemVariable variable = new SystemVariable(sessionKey, new StringLiteral(value));
        VariableMgr.checkSystemVariableExist(variable);

        // check whether the value is valid
        Field field = VariableMgr.getField(sessionKey);
        if (field == null || !canAssignValue(field, value)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, value);
        }

        // check flags of the variable, e.g. whether the variable is read-only
        VariableMgr.checkUpdate(variable);
    }

    // check whether the catalog exist
    private void checkCatalog(String catalogName) throws DdlException {
        if (catalogName.equalsIgnoreCase(EMPTY_VALUE)) {
            return;
        }

        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }
        }
    }

    // check whether the database exist
    // we need to reset the database if it checks failed
    private void checkDatabase(String newCatalog, String newDatabase) {
        if (newDatabase.equalsIgnoreCase(DATABASE_DEFAULT_VALUE)) {
            return;
        }

        // check whether the database exists
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Database db = metadataMgr.getDb(newCatalog, newDatabase);
        if (db == null) {
            String catalogDbName = newCatalog + "." + newDatabase;
            throw new StarRocksConnectorException(catalogDbName + " not exists");
        }
    }


    public static List<Pair<String, String>> changeToPairList(Map<String, String> properties) {
        List<Pair<String, String>> list = Lists.newArrayList();
        if (properties == null || properties.size() == 0) {
            return list;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            list.add(Pair.create(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    private boolean canAssignValue(Field field, String value) {
        Class<?> fieldType = field.getType();
        try {
            if (fieldType == int.class || fieldType == Integer.class) {
                Integer.parseInt(value);
            } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("Invalid boolean value");
                }
            } else if (fieldType == byte.class || fieldType == Byte.class) {
                Byte.parseByte(value);
            } else if (fieldType == short.class || fieldType == Short.class) {
                Short.parseShort(value);
            } else if (fieldType == long.class || fieldType == Long.class) {
                Long.parseLong(value);
            } else if (fieldType == float.class || fieldType == Float.class) {
                Float.parseFloat(value);
            } else if (fieldType == double.class || fieldType == Double.class) {
                Double.parseDouble(value);
            } else if (fieldType == String.class) {
                return true;
            } else {
                return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void setSessionVariable(String sessionKey, String value) {
        if (value.equalsIgnoreCase(EMPTY_VALUE)) {
            sessionVariables.remove(sessionKey);
        } else {
            sessionVariables.put(sessionKey, value);
        }
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        if (catalog.equalsIgnoreCase(EMPTY_VALUE)) {
            this.catalog = CATALOG_DEFAULT_VALUE;
        } else {
            this.catalog = catalog;
        }
    }

    private long checkMaxConn(String value) throws DdlException {
        if (value.equalsIgnoreCase(EMPTY_VALUE)) {
            return MAX_CONN_DEFAULT_VALUE;
        }

        try {
            long newMaxConn = Long.parseLong(value);

            if (newMaxConn <= 0 || newMaxConn > 10000) {
                throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not valid, the value must be between 1 and 10000");
            }

            if (newMaxConn > Config.qe_max_connection) {
                throw new DdlException(
                        PROP_MAX_USER_CONNECTIONS + " is not valid, the value must be less than qe_max_connection(" +
                                Config.qe_max_connection + ")");
            }

            return newMaxConn;
        } catch (NumberFormatException e) {
            throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not a number");
        }
    }

    private void setMaxConn(long value) {
        maxConn = value;
    }
}
