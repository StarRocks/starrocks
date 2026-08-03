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
package com.starrocks.common.util;

import com.starrocks.common.Config;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProfileManagerTest {

    public RuntimeProfile buildRuntimeProfile(String queryId, String queryType) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, queryType);

        profile.addChild(summaryProfile);

        return profile;
    }

    @Test
    public void testSingleton() {
        ProfileManager instance1 = ProfileManager.getInstance();
        ProfileManager instance2 = ProfileManager.getInstance();
        assertSame(instance1, instance2, "ProfileManager should be singleton");
    }

    @Test
    public void testProfileAddAndGet() {
        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfile("123", "Query");
        manager.pushProfile(null, profile);

        String retrievedProfile = manager.getProfile("123");
        assertNotNull(retrievedProfile, "Retrieved profile should not be null");
        assertTrue(manager.hasProfile("123"), "Profile should exist");

        assertEquals(1, manager.getAllProfileElements().size());

        assertNotNull(manager.getProfileElement("123"), "Profile element should not be null");

        manager.clearProfiles();
    }

    @Test
    public void testRemoveProfile() {
        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfile("124", "Load");
        manager.pushProfile(null, profile);

        String retrievedProfile = manager.getProfile("124");
        assertNotNull(retrievedProfile, "Retrieved profile should not be null");

        manager.removeProfile("124");
        assertNull(manager.getProfile("124"), "Profile should be removed");
        assertFalse(manager.hasProfile("124"), "Profile should not exist");
    }

    @Test
    public void testGetAllQueries() {
        ProfileManager manager = ProfileManager.getInstance();
        assertTrue(manager.getAllProfileElements().isEmpty());

        RuntimeProfile profile1 = buildRuntimeProfile("123", "Query");
        manager.pushProfile(null, profile1);

        RuntimeProfile profile2 = buildRuntimeProfile("124", "Load");
        manager.pushProfile(null, profile2);

        assertEquals(2, manager.getAllQueries().size());

        manager.clearProfiles();
    }

    private RuntimeProfile buildLoadProfile(String queryId, String state) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, state);
        summaryProfile.addInfoString(ProfileManager.START_TIME, "2024-01-01 10:00:00");
        summaryProfile.addInfoString(ProfileManager.END_TIME, "2024-01-01 10:00:05");
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, "test_db");
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, "stream load label");
        summaryProfile.addInfoString(ProfileManager.USER, "root");
        summaryProfile.addInfoString(ProfileManager.WAREHOUSE_CNGROUP, "default_warehouse");
        profile.addChild(summaryProfile);
        return profile;
    }

    @Test
    public void testPrintLoadProfileToLog() {
        ProfileManager manager = ProfileManager.getInstance();
        boolean original = Config.enable_print_load_profile_to_log;
        Config.enable_print_load_profile_to_log = true;
        try {
            // Each state exercises buildLoadQueryDetail plus one branch of toLoadState:
            // Finished -> FINISHED, Aborted -> FAILED, CANCELLED -> CANCELLED, Running -> RUNNING (default).
            String[][] loads = {
                    {"load-finished", "Finished"},
                    {"load-aborted", "Aborted"},
                    {"load-cancelled", "CANCELLED"},
                    {"load-running", "Running"}};
            for (String[] load : loads) {
                manager.pushProfile(null, buildLoadProfile(load[0], load[1]));
                assertTrue(manager.hasProfile(load[0]), "Load profile should be stored");
            }

            // A query profile must not take the load-profile log path but is still stored normally.
            manager.pushProfile(null, buildRuntimeProfile("a-query", "Query"));
            assertTrue(manager.hasProfile("a-query"), "Query profile should be stored");
        } finally {
            Config.enable_print_load_profile_to_log = original;
            manager.clearProfiles();
        }
    }

    @Test
    public void testPushExceed() {
        ProfileManager manager = ProfileManager.getInstance();
        assertTrue(manager.getAllProfileElements().isEmpty());

        Config.profile_info_reserved_num = 1;

        RuntimeProfile profile1 = buildRuntimeProfile("123", "Query");
        manager.pushProfile(null, profile1);

        RuntimeProfile profile2 = buildRuntimeProfile("124", "Query");
        manager.pushProfile(null, profile2);

        assertEquals(1, manager.getAllQueries().size());

        manager.clearProfiles();
    }
}

