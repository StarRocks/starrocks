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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Dictionary implements Writable, GsonPostProcessable {
    // Dictionary properties
    public static final String PROPERTIES_DICTIONARY_WARM_UP = "dictionary_warm_up";
    public static final String PROPERTIES_DICTIONARY_MEMORY_LIMIT = "dictionary_memory_limit";
    public static final String PROPERTIES_DICTIONARY_REFRESH_INTERVAL = "dictionary_refresh_interval";
    public static final String PROPERTIES_DICTIONARY_READ_LATEST = "dictionary_read_latest";
    public static final String PROPERTIES_DICTIONARY_IGNORE_FAILED_REFRESH = "dictionary_ignore_failed_refresh";

    public enum DictionaryState {
        UNINITIALIZED,
        REFRESHING,
        COMMITTING,
        FINISHED,
        CANCELLED
    }

    @SerializedName(value = "dictionaryId")
    private long dictionaryId;
    @SerializedName(value = "dictionaryName")
    private String dictionaryName;
    @SerializedName(value = "dbName")
    private String dbName;
    @SerializedName(value = "queryableObject")
    private String queryableObject;

    @SerializedName(value = "dictionaryKeys")
    private List<String> dictionaryKeys = Lists.newArrayList();
    @SerializedName(value = "dictionaryValues")
    private List<String> dictionaryValues = Lists.newArrayList();
    private AtomicLong nextSchedulableTime = new AtomicLong(Long.MAX_VALUE); // ms

    private Map<String, String> properties = Maps.newHashMap();

    private long lastSuccessRefreshTime = 0;
    private long lastSuccessFinishedTime = 0;
    private DictionaryState state = DictionaryState.UNINITIALIZED;
    private DictionaryState stateBeforeRefresh = null;
    private String runtimeErrMsg;

    public Dictionary(long dictionaryId, String dictionaryName, String queryableObject,
                      String dbName, List<String> dictionaryKeys, List<String> dictionaryValues,
                      Map<String, String> properties) {
        this.dictionaryId = dictionaryId;
        this.dictionaryName = dictionaryName;
        this.dbName = dbName;
        this.queryableObject = queryableObject;
        this.dictionaryKeys = dictionaryKeys;
        this.dictionaryValues = dictionaryValues;
        this.properties = properties;
        this.runtimeErrMsg = "";
    }

    public Dictionary() {}

    // properties
    private boolean warmUp = true; // only use when create dictionary but not in restart
    @SerializedName(value = "dictionaryMemoryLimit")
    private long memoryLimit = 2147483648L; // 2G by default
    @SerializedName(value = "dictionaryRefreshInterval")
    private int refreshInterval = 0; // ms, if <= 0, means do not refresh automatically
    @SerializedName(value = "dictionaryReadLatest")
    private boolean readLatest = false;
    @SerializedName(value = "dictionaryIgnoreFailedRefresh")
    private boolean ignoreFailedRefresh = false;

    public long getDictionaryId() {
        return dictionaryId;
    }

    public String getDictionaryName() {
        return dictionaryName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getQueryableObject() {
        return queryableObject;
    }

    public List<String> getKeys() {
        return dictionaryKeys;
    }

    public List<String> getValues() {
        return dictionaryValues;
    }

    public boolean needWarmUp() {
        return warmUp;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public int getRefreshInterval() {
        return refreshInterval;
    }

    public long getNextSchedulableTime() {
        return nextSchedulableTime.get();
    }

    public void updateNextSchedulableTime(long refreshInterval) {
        if (refreshInterval > 0) {
            nextSchedulableTime.set(System.currentTimeMillis() + refreshInterval);
        }
    }

    public boolean getReadLatest() {
        return readLatest;
    }

    public boolean getIgnoreFailedRefresh() {
        return ignoreFailedRefresh;
    }

    private void buildWarmUp(String value) throws DdlException {
        if (value.equalsIgnoreCase("TRUE")) {
            warmUp = true;
        } else if (value.equalsIgnoreCase("FALSE")) {
            warmUp = false;
        } else {
            throw new DdlException("parse dictionary_warm_up failed" +
                                   ", given parameter: " + value);
        }
    }

    private void buildMemoryLimit(String value) throws DdlException {
        long factor = 1;
        boolean truncateLastTwoWords = false;
        boolean truncateLastWord = false;
        if (value.length() > 1) {
            String lastWord = value.substring(value.length() - 1);
            String lastTwoWords = value.substring(value.length() - 2);

            if (lastTwoWords.equalsIgnoreCase("KB")) {
                truncateLastTwoWords = true;
                factor = 1024;
            } else if (lastTwoWords.equalsIgnoreCase("MB")) {
                truncateLastTwoWords = true;
                factor = 1024 * 1024;
            } else if (lastTwoWords.equalsIgnoreCase("GB")) {
                truncateLastTwoWords = true;
                factor = 1024 * 1024 * 1024;
            } else if (lastWord.equalsIgnoreCase("B")) {
                truncateLastWord = true;
                factor = 1;
            } else if (lastWord.equalsIgnoreCase("K")) {
                truncateLastWord = true;
                factor = 1024;
            } else if (lastWord.equalsIgnoreCase("M")) {
                truncateLastWord = true;
                factor = 1024 * 1024;
            } else if (lastWord.equalsIgnoreCase("G")) {
                truncateLastWord = true;
                factor = 1024 * 1024 * 1024;
            }
        }

        try {
            String parseString = "";
            if (truncateLastTwoWords) {
                parseString = value.substring(0, value.length() - 2);
            } else if (truncateLastWord) {
                parseString = value.substring(0, value.length() - 1);
            } else {
                parseString = value;
            }
            memoryLimit = Long.parseLong(parseString) * factor;
        } catch (Exception e) {
            throw new DdlException("parse dictionary_memory_limit failed" +
                                   ", given parameter: " + value);
        }
        return;
    }

    private void buildRefreshInterval(String value) throws DdlException {
        try {
            refreshInterval = Integer.parseInt(value) * 1000;
        } catch (Exception e) {
            throw new DdlException("parse dictionary_refresh_interval failed" +
                                   ", given parameter: " + value);
        }
    }

    private void buildReadLatest(String value) throws DdlException {
        if (value.equalsIgnoreCase("TRUE")) {
            readLatest = true;
        } else if (value.equalsIgnoreCase("FALSE")) {
            readLatest = false;
        } else {
            throw new DdlException("parse dictionary_read_latest failed" +
                                   ", given parameter: " + value);
        }
    }

    private void buildIgnoreFailedRefresh(String value) throws DdlException {
        if (value.equalsIgnoreCase("TRUE")) {
            ignoreFailedRefresh = true;
        } else if (value.equalsIgnoreCase("FALSE")) {
            ignoreFailedRefresh = false;
        } else {
            throw new DdlException("parse dictionary_ignore_failed_refresh failed" +
                                   ", given parameter: " + value);
        }
    }

    public void buildDictionaryProperties() throws DdlException {
        if (properties == null || properties.size() == 0) {
            return;
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();

            switch (key) {
                case PROPERTIES_DICTIONARY_WARM_UP:
                    buildWarmUp(value);
                    break;
                case PROPERTIES_DICTIONARY_MEMORY_LIMIT:
                    buildMemoryLimit(value);
                    break;
                case PROPERTIES_DICTIONARY_REFRESH_INTERVAL:
                    buildRefreshInterval(value);
                    break;
                case PROPERTIES_DICTIONARY_READ_LATEST:
                    buildReadLatest(value);
                    break;
                case PROPERTIES_DICTIONARY_IGNORE_FAILED_REFRESH:
                    buildIgnoreFailedRefresh(value);
                    break;
                default:
                    throw new DdlException("unknown property for dictionary: " + key);
            }
        }
        return;
    }

    public String buildQuery() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");

        List<String> allColumns = new ArrayList<>();
        allColumns.addAll(dictionaryKeys);
        allColumns.addAll(dictionaryValues);

        for (int i = 0; i < allColumns.size(); ++i) {
            String value = allColumns.get(i);
            sb.append(value);
            if (i != allColumns.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append(" FROM " + queryableObject + ";");
        return sb.toString();
    }

    public synchronized void resetState() {
        this.setErrorMsg("");
        this.state = DictionaryState.UNINITIALIZED;
        this.stateBeforeRefresh = null;
    }

    public synchronized void setRefreshing() {
        this.stateBeforeRefresh = this.state;
        this.state = DictionaryState.REFRESHING;
        this.lastSuccessRefreshTime = System.currentTimeMillis();
        this.setErrorMsg("");
    }

    public synchronized void setCommitting() {
        this.state = DictionaryState.COMMITTING;
        this.stateBeforeRefresh = null;
    }

    public synchronized void setFinished() {
        this.state = DictionaryState.FINISHED;
        this.lastSuccessFinishedTime = System.currentTimeMillis();
        this.stateBeforeRefresh = null;
    }

    public synchronized void setCancelled() {
        this.state = DictionaryState.CANCELLED;
        this.stateBeforeRefresh = null;
    }

    public DictionaryState getState() {
        return state;
    }

    public synchronized void setErrorMsg(String msg) {
        runtimeErrMsg = msg;
    }

    public synchronized void resetStateBeforeRefresh() {
        if (this.stateBeforeRefresh != null) {
            this.state = this.stateBeforeRefresh;
            this.stateBeforeRefresh = null;
        }
    }

    public List<String> getInfo() {
        List<String> info = new ArrayList<>();
        info.add(String.valueOf(dictionaryId));
        info.add(dictionaryName);
        info.add(dbName);
        info.add(queryableObject);

        String keys = "";
        String values = "";
        for (int i = 0; i < dictionaryKeys.size(); ++i) {
            if (i == 0) {
                keys += "[";
            }

            keys += dictionaryKeys.get(i);
            if (i == dictionaryKeys.size() - 1) {
                keys += "]";
            } else {
                keys += ", ";
            }
        }

        for (int i = 0; i < dictionaryValues.size(); ++i) {
            if (i == 0) {
                values += "[";
            }

            values += dictionaryValues.get(i);
            if (i == dictionaryValues.size() - 1) {
                values += "]";
            } else {
                values += ", ";
            }
        }
        info.add(keys);
        info.add(values);
        info.add(state.name());
        info.add(TimeUtils.longToTimeString(lastSuccessRefreshTime));
        info.add(TimeUtils.longToTimeString(lastSuccessFinishedTime));
        if (refreshInterval > 0) {
            info.add(TimeUtils.longToTimeString(getNextSchedulableTime()));
        } else {
            info.add("disable auto schedule for refreshing");
        }
        info.add(runtimeErrMsg);
        return info;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.resetState();
        this.updateNextSchedulableTime(this.getRefreshInterval());
    }

    public static Dictionary read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Dictionary.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
}
