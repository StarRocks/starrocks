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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/VariableMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.PatternMatcher;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Variable manager, merge session variable and global variable.
 * <p>
 * There are two types of variables, SESSION and GLOBAL.
 * <p>
 * The GLOBAL variable is more like a system configuration, which takes effect globally.
 * The settings for global variables are global and persistent.
 * After the cluster is restarted, the set values still be restored.
 * The global variables are defined in `GlobalVariable`.
 * The variable of the READ_ONLY attribute cannot be changed,
 * and the variable of the GLOBAL attribute can be changed at runtime.
 * <p>
 * Session variables are session-level, and the scope of these variables is usually
 * in a session connection. The session variables are defined in `SessionVariable`.
 * <p>
 * For the setting of the global variable, the value of the field in the `GlobalVariable` class
 * will be modified directly through the reflection mechanism of Java.
 * <p>
 * For the setting of session variables, there are also two types: Global and Session.
 * <p>
 * 1. Use `set global` comment to set session variables
 * <p>
 * This setting method is equivalent to changing the default value of the session variable.
 * It will modify the `defaultSessionVariable` member.
 * This operation is persistent and global. After the setting is complete, when a new session
 * is established, this default value will be used to generate session-level session variables.
 * This operation will also affect the value of the variable in the current session.
 * <p>
 * 2. Use the `set` comment (no global) to set the session variable
 * <p>
 * This setting method will only change the value of the variable in the current session.
 * After the session ends, this setting will also become invalid.
 */
public class VariableMgr {
    private static final Logger LOG = LogManager.getLogger(VariableMgr.class);

    // variable have this flag means that every session have a copy of this variable,
    // and can modify its own variable.
    public static final int SESSION = 1;
    // Variables with this flag have only one instance in one process.
    public static final int GLOBAL = 1 << 1;
    // Variables with this flag only exist in each session.
    public static final int SESSION_ONLY = 1 << 2;
    // Variables with this flag can only be read.
    public static final int READ_ONLY = 1 << 3;
    // Variables with this flag can not be seen with `SHOW VARIABLES` statement.
    public static final int INVISIBLE = 1 << 4;
    // Variables with this flag will not forward to leader when modified in session
    public static final int DISABLE_FORWARD_TO_LEADER = 1 << 5;

    // Map variable name to variable context which have enough information to change variable value.
    // This map contains info of all session and global variables.
    private static final ImmutableMap<String, VarContext> CTX_BY_VAR_NAME;

    private static final ImmutableMap<String, String> ALIASES;

    // This variable is equivalent to the default value of session variables.
    // Whenever a new session is established, the value in this object is copied to the session-level variable.
    private static final SessionVariable DEFAULT_SESSION_VARIABLE;

    // Global read/write lock to protect access of globalSessionVariable.
    private static final ReadWriteLock RWLOCK = new ReentrantReadWriteLock();
    private static final Lock RLOCK = RWLOCK.readLock();
    private static final Lock WLOCK = RWLOCK.writeLock();

    // Form map from variable name to its field in Java class.
    static {
        // Session value
        DEFAULT_SESSION_VARIABLE = new SessionVariable();
        ImmutableSortedMap.Builder<String, VarContext> ctxBuilder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        ImmutableSortedMap.Builder<String, String> aliasBuilder =
                ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (Field field : SessionVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }

            if (StringUtils.isNotBlank(attr.show())) {
                Preconditions.checkState((attr.show().equals(attr.name()) || attr.show().equals(attr.alias())),
                        "Session variables show is not equal name or alias");
            }

            field.setAccessible(true);
            ctxBuilder.put(attr.name(), new VarContext(field, DEFAULT_SESSION_VARIABLE, SESSION | attr.flag(),
                    getValue(DEFAULT_SESSION_VARIABLE, field), attr));

            if (!attr.alias().isEmpty()) {
                aliasBuilder.put(attr.alias(), attr.name());
            }
        }

        // Variables only exist in global environment.
        for (Field field : GlobalVariable.class.getDeclaredFields()) {
            VarAttr attr = field.getAnnotation(VarAttr.class);
            if (attr == null) {
                continue;
            }

            field.setAccessible(true);
            ctxBuilder.put(attr.name(),
                    new VarContext(field, null, GLOBAL | attr.flag(), getValue(null, field), attr));

            if (!attr.alias().isEmpty()) {
                aliasBuilder.put(attr.alias(), attr.name());
            }
        }

        CTX_BY_VAR_NAME = ctxBuilder.build();
        ALIASES = aliasBuilder.build();
    }

    public static SessionVariable getDefaultSessionVariable() {
        return DEFAULT_SESSION_VARIABLE;
    }

    // Set value to a variable
    private static boolean setValue(Object obj, Field field, String value) throws DdlException {
        VarAttr attr = field.getAnnotation(VarAttr.class);

        String variableName;
        if (attr.show().isEmpty()) {
            variableName = attr.name();
        } else {
            variableName = attr.show();
        }

        String convertedVal = VariableVarConverters.convert(variableName, value);
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    if (convertedVal.equalsIgnoreCase("ON")
                            || convertedVal.equalsIgnoreCase("TRUE")
                            || convertedVal.equalsIgnoreCase("1")) {
                        field.setBoolean(obj, true);
                    } else if (convertedVal.equalsIgnoreCase("OFF")
                            || convertedVal.equalsIgnoreCase("FALSE")
                            || convertedVal.equalsIgnoreCase("0")) {
                        field.setBoolean(obj, false);
                    } else {
                        throw new IllegalAccessException();
                    }
                    break;
                case "byte":
                    field.setByte(obj, Byte.parseByte(convertedVal));
                    break;
                case "short":
                    field.setShort(obj, Short.parseShort(convertedVal));
                    break;
                case "int":
                    field.setInt(obj, Integer.parseInt(convertedVal));
                    break;
                case "long":
                    field.setLong(obj, Long.parseLong(convertedVal));
                    break;
                case "float":
                    field.setFloat(obj, Float.parseFloat(convertedVal));
                    break;
                case "double":
                    field.setDouble(obj, Double.parseDouble(convertedVal));
                    break;
                case "String":
                    field.set(obj, convertedVal);
                    break;
                default:
                    // Unsupported type variable.
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, variableName);
            }
        } catch (NumberFormatException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, variableName);
        } catch (IllegalAccessException e) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_VALUE_FOR_VAR, variableName, value);
        }

        return true;
    }

    public static SessionVariable newSessionVariable() {
        try {
            return (SessionVariable) DEFAULT_SESSION_VARIABLE.clone();
        } catch (CloneNotSupportedException e) {
            LOG.warn(e);
        }
        return null;
    }

    // Check if this setVar can be set correctly
    private static void checkUpdate(SystemVariable setVar, int flag) throws DdlException {
        if ((flag & READ_ONLY) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_VARIABLE_IS_READONLY, setVar.getVariable());
        }
        if (setVar.getType() == SetType.GLOBAL && (flag & SESSION_ONLY) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_LOCAL_VARIABLE, setVar.getVariable());
        }
        if (setVar.getType() != SetType.GLOBAL && (flag & GLOBAL) != 0) {
            ErrorReport.reportDdlException(ErrorCode.ERR_GLOBAL_VARIABLE, setVar.getVariable());
        }
    }

    // Entry of handling SetVarStmt
    // Input:
    //      sessionVariable: the variable of current session
    //      setVar: variable information that needs to be set
    public static void setSystemVariable(SessionVariable sessionVariable, SystemVariable setVar, boolean onlySetSessionVar)
            throws DdlException {
        if (SessionVariable.DEPRECATED_VARIABLES.stream().anyMatch(c -> c.equalsIgnoreCase(setVar.getVariable()))) {
            return;
        }

        VarContext ctx = getVarContext(setVar.getVariable());
        if (ctx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, setVar.getVariable());
        }

        if (setVar.getType() == SetType.VERBOSE) {
            ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_TYPE_FOR_VAR, setVar.getVariable());
        }

        // Check variable attribute and setVar
        checkUpdate(setVar, ctx.getFlag());

        // To modify to default value.
        VarAttr attr = ctx.getField().getAnnotation(VarAttr.class);
        String value;
        // If value is null, this is `set variable = DEFAULT`
        if (setVar.getResolvedExpression() != null) {
            value = setVar.getResolvedExpression().getStringValue();
        } else {
            value = ctx.getDefaultValue();
            if (value == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NO_DEFAULT, attr.name());
            }
        }

        if (!onlySetSessionVar && setVar.getType() == SetType.GLOBAL) {
            WLOCK.lock();
            try {
                setValue(ctx.getObj(), ctx.getField(), value);
                // write edit log
                GlobalVarPersistInfo info =
                        new GlobalVarPersistInfo(DEFAULT_SESSION_VARIABLE, Lists.newArrayList(attr.name()));
                EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
                editLog.logGlobalVariableV2(info);
            } finally {
                WLOCK.unlock();
            }
        }

        // set session variable
        setValue(sessionVariable, ctx.getField(), value);
    }

    // global variable persistence
    public static void write(DataOutputStream out) throws IOException {
        DEFAULT_SESSION_VARIABLE.write(out);
        // get all global variables
        List<String> varNames = GlobalVariable.getAllGlobalVarNames();
        GlobalVarPersistInfo info = new GlobalVarPersistInfo(DEFAULT_SESSION_VARIABLE, varNames);
        info.write(out);
    }

    public static void read(DataInputStream in) throws IOException, DdlException {
        WLOCK.lock();
        try {
            DEFAULT_SESSION_VARIABLE.readFields(in);
            if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_90) {
                GlobalVarPersistInfo info = GlobalVarPersistInfo.read(in);
                replayGlobalVariableV2(info);
            }
        } finally {
            WLOCK.unlock();
        }
    }

    public static void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        try {
            int sessionVarSize = reader.readInt();
            for (int i = 0; i < sessionVarSize; ++i) {
                VariableInfo v = reader.readJson(VariableInfo.class);
                VarContext varContext = getVarContext(v.name);
                if (varContext != null) {
                    setValue(varContext.getObj(), varContext.getField(), v.variable);
                }
            }

            int globalVarSize = reader.readInt();
            for (int i = 0; i < globalVarSize; ++i) {
                VariableInfo v = reader.readJson(VariableInfo.class);
                VarContext varContext = getVarContext(v.name);
                if (varContext != null) {
                    setValue(varContext.getObj(), varContext.getField(), v.variable);
                }
            }
        } catch (DdlException e) {
            throw new IOException(e);
        }
    }

    @Deprecated
    public static void replayGlobalVariable(SessionVariable variable) throws DdlException {
        WLOCK.lock();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }

                field.setAccessible(true);

                VarContext ctx = getVarContext(attr.name());
                if (ctx.getFlag() == SESSION) {
                    String value = getValue(variable, ctx.getField());
                    setValue(ctx.getObj(), ctx.getField(), value);
                }
            }
        } finally {
            WLOCK.unlock();
        }
    }

    // this method is used to replace the `replayGlobalVariable()`
    public static void replayGlobalVariableV2(GlobalVarPersistInfo info) throws DdlException {
        WLOCK.lock();
        try {
            String json = info.getPersistJsonString();
            JSONObject root = new JSONObject(json);
            for (String varName : root.keySet()) {
                VarContext varContext = getVarContext(varName);
                if (varContext == null) {
                    LOG.error("failed to get global variable {} when replaying", varName);
                    continue;
                }
                setValue(varContext.getObj(), varContext.getField(), root.get(varName).toString());
            }
        } finally {
            WLOCK.unlock();
        }
    }

    // Get variable value through variable name, used to satisfy statement like `SELECT @@comment_version`
    public static void fillValue(SessionVariable var, VariableExpr desc) throws AnalysisException {
        VarContext ctx = getVarContext(desc.getName());
        if (ctx == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, desc.getName());
        }

        if (desc.getSetType() == SetType.GLOBAL) {
            RLOCK.lock();
            try {
                fillValue(ctx.getObj(), ctx.getField(), desc);
            } finally {
                RLOCK.unlock();
            }
        } else {
            fillValue(var, ctx.getField(), desc);
        }
    }

    private static void fillValue(Object obj, Field field, VariableExpr desc) {
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    desc.setType(Type.BOOLEAN);
                    desc.setValue(field.getBoolean(obj));
                    break;
                case "byte":
                    desc.setType(Type.TINYINT);
                    desc.setValue(field.getByte(obj));
                    break;
                case "short":
                    desc.setType(Type.SMALLINT);
                    desc.setValue(field.getShort(obj));
                    break;
                case "int":
                    desc.setType(Type.INT);
                    desc.setValue(field.getInt(obj));
                    break;
                case "long":
                    desc.setType(Type.BIGINT);
                    desc.setValue(field.getLong(obj));
                    break;
                case "float":
                    desc.setType(Type.FLOAT);
                    desc.setValue(field.getFloat(obj));
                    break;
                case "double":
                    desc.setType(Type.DOUBLE);
                    desc.setValue(field.getDouble(obj));
                    break;
                case "String":
                    desc.setType(Type.VARCHAR);
                    desc.setValue((String) field.get(obj));
                    break;
                default:
                    desc.setType(Type.VARCHAR);
                    desc.setValue("");
                    break;
            }
        } catch (IllegalAccessException e) {
            LOG.warn("Access failed.", e);
        }
    }

    // Get variable value through variable name, used to satisfy statement like `SELECT @@comment_version`
    public static String getValue(SessionVariable var, VariableExpr desc) throws AnalysisException {
        VarContext ctx = getVarContext(desc.getName());
        if (ctx == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, desc.getName());
        }

        if (desc.getSetType() == SetType.GLOBAL) {
            RLOCK.lock();
            try {
                return getValue(ctx.getObj(), ctx.getField());
            } finally {
                RLOCK.unlock();
            }
        } else {
            return getValue(var, ctx.getField());
        }
    }

    private static String getValue(Object obj, Field field) {
        try {
            switch (field.getType().getSimpleName()) {
                case "boolean":
                    return Boolean.toString(field.getBoolean(obj));
                case "byte":
                    return Byte.toString(field.getByte(obj));
                case "short":
                    return Short.toString(field.getShort(obj));
                case "int":
                    return Integer.toString(field.getInt(obj));
                case "long":
                    return Long.toString(field.getLong(obj));
                case "float":
                    return Float.toString(field.getFloat(obj));
                case "double":
                    return Double.toString(field.getDouble(obj));
                case "String":
                    return (String) field.get(obj);
                default:
                    return "";
            }
        } catch (IllegalAccessException e) {
            LOG.warn("Access failed.", e);
        }
        return "";
    }

    public static String getDefaultValue(String variable) {
        VarContext ctx = getVarContext(variable);
        if (ctx == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_SYSTEM_VARIABLE, variable);
        }

        String value = ctx.getDefaultValue();
        if (value == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DEFAULT, variable);
        }
        return value;
    }

    // Dump all fields. Used for `show variables`, but note `sessionVar` would be null.
    public static List<List<String>> dump(SetType type, SessionVariable sessionVar, PatternMatcher matcher) {
        List<List<String>> rows = Lists.newArrayList();
        // Hold the read lock when session dump, because this option need to access global variable.
        RLOCK.lock();
        try {
            for (Map.Entry<String, VarContext> entry : CTX_BY_VAR_NAME.entrySet()) {
                // Filter variable not match to the regex.
                String name = StringUtils.isBlank(entry.getValue().getVarAttr().show()) ? entry.getKey()
                        : entry.getValue().getVarAttr().show();

                if (matcher != null && !matcher.match(name)) {
                    continue;
                }
                VarContext ctx = entry.getValue();

                // For session variables, the flag is VariableMgr.SESSION | VariableMgr.INVISIBLE
                // For global variables, the flag is VariableMgr.GLOBAL | VariableMgr.INVISIBLE
                if ((ctx.getFlag() > VariableMgr.INVISIBLE) && sessionVar != null &&
                        !sessionVar.isEnableShowAllVariables()) {
                    continue;
                }

                List<String> row = Lists.newArrayList();
                if (type != SetType.GLOBAL && ctx.getObj() == DEFAULT_SESSION_VARIABLE) {
                    // In this condition, we may retrieve session variables for caller.
                    if (sessionVar != null) {
                        row.add(name);
                        String currentValue = getValue(sessionVar, ctx.getField());
                        row.add(currentValue);
                        if (type == SetType.VERBOSE) {
                            row.add(ctx.defaultValue);
                            row.add(ctx.defaultValue.equals(currentValue) ? "0" : "1");
                        }
                    } else {
                        LOG.error("sessionVar is null during dumping session variables.");
                        continue;
                    }
                } else {
                    row.add(name);
                    String currentValue = getValue(ctx.getObj(), ctx.getField());
                    row.add(currentValue);
                    if (type == SetType.VERBOSE) {
                        row.add(ctx.defaultValue);
                        row.add(ctx.defaultValue.equals(currentValue) ? "0" : "1");
                    }
                }

                if (row.get(0).equalsIgnoreCase(SessionVariable.SQL_MODE)) {
                    try {
                        row.set(1, SqlModeHelper.decode(Long.valueOf(row.get(1))));
                    } catch (DdlException e) {
                        row.set(1, "");
                        LOG.warn("Decode sql mode failed");
                    }
                }

                rows.add(row);
            }
        } finally {
            RLOCK.unlock();
        }

        // Sort all variables by variable name.
        rows.sort(Comparator.comparing(o -> o.get(0)));

        return rows;
    }

    // global variable persistence
    public static long loadGlobalVariable(DataInputStream in, long checksum) throws IOException, DdlException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_22) {
            read(in);
        }
        LOG.info("finished replay globalVariable from image");
        return checksum;
    }

    public static long saveGlobalVariable(DataOutputStream out, long checksum) throws IOException {
        VariableMgr.write(out);
        return checksum;
    }

    public static boolean shouldForwardToLeader(String name) {
        VarContext varContext = getVarContext(name);
        if (varContext == null) {
            // DEPRECATED_VARIABLES like enable_cbo don't have flag
            // we simply assume they all can forward to leader
            return true;
        } else {
            return (varContext.getFlag() & DISABLE_FORWARD_TO_LEADER) == 0;
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface VarAttr {
        // Name in show variables and set statement;
        String name();

        String alias() default "";

        // Use in show variables, must keep same with name or alias
        String show() default "";

        int flag() default 0;
    }

    private static class VarContext {
        private Field field;
        private Object obj;
        private int flag;
        private String defaultValue;
        private VarAttr varAttr;

        public VarContext(Field field, Object obj, int flag, String defaultValue, VarAttr varAttr) {
            this.field = field;
            this.obj = obj;
            this.flag = flag;
            this.defaultValue = defaultValue;
            this.varAttr = varAttr;
        }

        public Field getField() {
            return field;
        }

        public Object getObj() {
            return obj;
        }

        public int getFlag() {
            return flag;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public VarAttr getVarAttr() {
            return varAttr;
        }
    }

    private static VarContext getVarContext(String name) {
        VarContext ctx = CTX_BY_VAR_NAME.get(name);
        if (ctx == null) {
            ctx = CTX_BY_VAR_NAME.get(ALIASES.get(name));
        }
        return ctx;
    }

    private static class VariableInfo {
        @SerializedName(value = "n")
        private String name;
        @SerializedName(value = "v")
        private String variable;

        public VariableInfo(String name, String variable) {
            this.name = name;
            this.variable = variable;
        }
    }
}
