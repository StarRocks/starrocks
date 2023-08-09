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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/ProfileManager.java

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

package com.starrocks.common.util;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/*
 * if you want to visit the atrribute(such as queryID,defaultDb)
 * you can use profile.getInfoStrings("queryId")
 * All attributes can be seen from the above.
 *
 * why the element in the finished profile arary is not RuntimeProfile,
 * the purpose is let coordinator can destruct earlier(the fragment profile is in Coordinator)
 *
 */
public class ProfileManager {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static ProfileManager INSTANCE = null;
    public static final String QUERY_ID = "Query ID";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String QUERY_TYPE = "Query Type";
    public static final String QUERY_STATE = "Query State";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String VARIABLES = "Variables";
    public static final String PROFILE_TIME = "Collect Profile Time";

    public static final ArrayList<String> PROFILE_HEADERS = new ArrayList<>(
            Arrays.asList(QUERY_ID, USER, DEFAULT_DB, SQL_STATEMENT, QUERY_TYPE,
                    START_TIME, END_TIME, TOTAL_TIME, QUERY_STATE));

    public static class ProfileElement {
        public Map<String, String> infoStrings = Maps.newHashMap();
        public byte[] profileContent;
        public ProfilingExecPlan plan;

        public List<String> toRow() {
            List<String> res = Lists.newArrayList();
            res.add(infoStrings.get(QUERY_ID));
            res.add(infoStrings.get(START_TIME));
            res.add(infoStrings.get(TOTAL_TIME));
            res.add(infoStrings.get(QUERY_STATE));
            String statement = infoStrings.get(SQL_STATEMENT);
            if (statement.length() > 128) {
                statement = statement.substring(0, 124) + " ...";
            }
            res.add(statement);
            return res;
        }
    }

    private final ReadLock readLock;
    private final WriteLock writeLock;

    private final LinkedHashMap<String, ProfileElement> profileMap; // from QueryId to RuntimeProfile
    private final LinkedHashMap<String, ProfileElement> loadProfileMap; // from LoadId to RuntimeProfile

    public static ProfileManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProfileManager();
        }
        return INSTANCE;
    }

    private ProfileManager() {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        profileMap = new LinkedHashMap<>();
        loadProfileMap = new LinkedHashMap<>();
    }

    public ProfileElement createElement(RuntimeProfile summaryProfile, String profileString) {
        ProfileElement element = new ProfileElement();
        for (String header : PROFILE_HEADERS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        try {
            element.profileContent = CompressionUtils.gzipCompressString(profileString);
        } catch (IOException e) {
            LOG.warn("Compress profile string failed, length: {}, reason: {}",
                    profileString.length(), e.getMessage());
        }
        return element;
    }

    private String generateProfileString(RuntimeProfile profile) {
        if (profile == null) {
            return "";
        }

        String profileString;
        switch (Config.profile_info_format) {
            case "default":
                profileString = profile.toString();
                break;
            case "json":
                RuntimeProfile.ProfileFormatter formatter = new RuntimeProfile.JsonProfileFormater();
                profileString = formatter.format(profile, "");
                break;
            default:
                profileString = profile.toString();
                LOG.warn("unknown profile format '{}',  use default format instead.", Config.profile_info_format);
        }
        return profileString;
    }

    public String pushProfile(ExecPlan plan, RuntimeProfile profile) {
        String profileString = generateProfileString(profile);
        ProfileElement element = createElement(profile.getChildList().get(0).first, profileString);
        element.plan = ProfilingExecPlan.buildFrom(plan);
        String queryId = element.infoStrings.get(ProfileManager.QUERY_ID);
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(queryId)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' column into infoStrings");
        }

        writeLock.lock();
        try {
            profileMap.put(queryId, element);
            if (profileMap.size() >= Config.profile_info_reserved_num) {
                profileMap.remove(profileMap.keySet().iterator().next());
            }
        } finally {
            writeLock.unlock();
        }

        return profileString;
    }

    public void pushLoadProfile(RuntimeProfile profile) {
        String profileString = generateProfileString(profile);

        ProfileElement element = createElement(profile.getChildList().get(0).first, profileString);
        String loadId = element.infoStrings.get(ProfileManager.QUERY_ID);
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(loadId)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' column into infoStrings");
        }

        writeLock.lock();
        try {
            loadProfileMap.put(loadId, element);
            if (loadProfileMap.size() >= Config.load_profile_info_reserved_num) {
                loadProfileMap.remove(loadProfileMap.keySet().iterator().next());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public List<List<String>> getAllQueries() {
        List<List<String>> result = Lists.newLinkedList();
        readLock.lock();
        try {
            for (ProfileElement element : profileMap.values()) {
                Map<String, String> infoStrings = element.infoStrings;
                List<String> row = Lists.newArrayList();
                for (String str : PROFILE_HEADERS) {
                    row.add(infoStrings.get(str));
                }
                result.add(0, row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    public String getProfile(String queryId) {
        ProfileElement element = new ProfileElement();
        readLock.lock();
        try {
            element = profileMap.get(queryId) == null ? loadProfileMap.get(queryId) : profileMap.get(queryId);
            if (element == null) {
                return null;
            }

            return CompressionUtils.gzipDecompressString(element.profileContent);
        } catch (IOException e) {
            LOG.warn("Decompress profile content failed, length: {}, reason: {}",
                    element.profileContent.length, e.getMessage());
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public ProfileElement getProfileElement(String queryId) {
        readLock.lock();
        try {
            return profileMap.get(queryId);
        } finally {
            readLock.unlock();
        }
    }

    public List<ProfileElement> getAllProfileElements() {
        List<ProfileElement> result = Lists.newArrayList();
        readLock.lock();
        try {
            result.addAll(profileMap.values());
        } finally {
            readLock.unlock();
        }
        return result;
    }
}
