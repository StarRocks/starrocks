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
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProfileManagerTest {

    @AfterEach
    public void cleanup() {
        ProfileManager.getInstance().clearProfiles();
        ConnectContext.remove();
    }

    public RuntimeProfile buildRuntimeProfile(String queryId, String queryType) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, queryType);

        profile.addChild(summaryProfile);

        return profile;
    }

    public RuntimeProfile buildRuntimeProfileWithStartTime(String queryId, String queryType, long startTimeMs) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, queryType);
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, "SELECT 1");
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(startTimeMs));
        summaryProfile.addInfoString(ProfileManager.START_TIME_MS, String.valueOf(startTimeMs));

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
    }

    /**
     * Tests that toRow() formats StartTime using UTC when session time_zone is UTC.
     * A fixed epoch ms (2024-01-01 00:00:00 UTC = 2024-01-01 08:00:00 Asia/Shanghai)
     * should appear as "2024-01-01 00:00:00" in UTC.
     */
    @Test
    public void testToRowStartTimeUsesSessionTimezoneUTC() {
        // 2024-01-01 00:00:00 UTC in epoch ms
        long startTimeMs = 1704067200000L;

        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfileWithStartTime("200", "Query", startTimeMs);
        manager.pushProfile(null, profile);

        // Set session timezone to UTC
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("UTC");
        ctx.setThreadLocalInfo();

        ProfileManager.ProfileElement element = manager.getProfileElement("200");
        assertNotNull(element);
        List<String> row = element.toRow();

        // StartTime column (index 1) should be in UTC: "2024-01-01 00:00:00"
        assertEquals("2024-01-01 00:00:00", row.get(1),
                "StartTime should be formatted in session timezone (UTC)");
    }

    /**
     * Tests that toRow() formats StartTime using Asia/Shanghai when session time_zone is Asia/Shanghai.
     * A fixed epoch ms (2024-01-01 00:00:00 UTC = 2024-01-01 08:00:00 Asia/Shanghai).
     */
    @Test
    public void testToRowStartTimeUsesSessionTimezoneShanghai() {
        // 2024-01-01 00:00:00 UTC in epoch ms
        long startTimeMs = 1704067200000L;

        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfileWithStartTime("201", "Query", startTimeMs);
        manager.pushProfile(null, profile);

        // Set session timezone to Asia/Shanghai
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("Asia/Shanghai");
        ctx.setThreadLocalInfo();

        ProfileManager.ProfileElement element = manager.getProfileElement("201");
        assertNotNull(element);
        List<String> row = element.toRow();

        // StartTime column (index 1) should be Asia/Shanghai: "2024-01-01 08:00:00"
        assertEquals("2024-01-01 08:00:00", row.get(1),
                "StartTime should be formatted in session timezone (Asia/Shanghai)");
    }

    /**
     * Tests that different sessions see different StartTime representations for the same profile.
     * Verifies that UTC and America/Los_Angeles show different values.
     */
    @Test
    public void testToRowStartTimeDiffersBySessionTimezone() {
        // 2024-06-15 12:00:00 UTC in epoch ms
        long startTimeMs = 1718452800000L;

        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfileWithStartTime("202", "Query", startTimeMs);
        manager.pushProfile(null, profile);

        ProfileManager.ProfileElement element = manager.getProfileElement("202");
        assertNotNull(element);

        // Check UTC
        ConnectContext ctxUTC = new ConnectContext();
        ctxUTC.getSessionVariable().setTimeZone("UTC");
        ctxUTC.setThreadLocalInfo();
        String startTimeUTC = element.toRow().get(1);
        assertEquals("2024-06-15 12:00:00", startTimeUTC,
                "StartTime should be 2024-06-15 12:00:00 in UTC");

        // Check America/Los_Angeles (UTC-7 during daylight saving in June)
        ConnectContext ctxLA = new ConnectContext();
        ctxLA.getSessionVariable().setTimeZone("America/Los_Angeles");
        ctxLA.setThreadLocalInfo();
        String startTimeLA = element.toRow().get(1);
        assertEquals("2024-06-15 05:00:00", startTimeLA,
                "StartTime should be 2024-06-15 05:00:00 in America/Los_Angeles (UTC-7 in summer)");

        assertFalse(startTimeUTC.equals(startTimeLA),
                "StartTime should differ between UTC and America/Los_Angeles sessions");
    }

    /**
     * Tests daylight saving time behavior: America/Los_Angeles is UTC-8 in winter, UTC-7 in summer.
     */
    @Test
    public void testToRowStartTimeDaylightSaving() {
        ProfileManager manager = ProfileManager.getInstance();

        // Winter: 2024-01-15 12:00:00 UTC -> 2024-01-15 04:00:00 PST (UTC-8)
        long winterMs = 1705320000000L;
        RuntimeProfile winterProfile = buildRuntimeProfileWithStartTime("203", "Query", winterMs);
        manager.pushProfile(null, winterProfile);

        // Summer: 2024-07-15 12:00:00 UTC -> 2024-07-15 05:00:00 PDT (UTC-7)
        long summerMs = 1721044800000L;
        RuntimeProfile summerProfile = buildRuntimeProfileWithStartTime("204", "Query", summerMs);
        manager.pushProfile(null, summerProfile);

        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("America/Los_Angeles");
        ctx.setThreadLocalInfo();

        ProfileManager.ProfileElement winterElement = manager.getProfileElement("203");
        assertNotNull(winterElement);
        assertEquals("2024-01-15 04:00:00", winterElement.toRow().get(1),
                "Winter time should use PST (UTC-8)");

        ProfileManager.ProfileElement summerElement = manager.getProfileElement("204");
        assertNotNull(summerElement);
        assertEquals("2024-07-15 05:00:00", summerElement.toRow().get(1),
                "Summer time should use PDT (UTC-7)");
    }

    /**
     * Tests backward compatibility: when START_TIME_MS is absent, toRow() falls back to the stored START_TIME string.
     */
    @Test
    public void testToRowFallbackWhenStartTimeMsAbsent() {
        ProfileManager manager = ProfileManager.getInstance();

        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, "205");
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, "SELECT 1");
        summaryProfile.addInfoString(ProfileManager.START_TIME, "2024-01-01 08:00:00");
        // Note: START_TIME_MS is intentionally NOT set
        profile.addChild(summaryProfile);
        manager.pushProfile(null, profile);

        // Set session timezone to UTC (different from stored Asia/Shanghai value)
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("UTC");
        ctx.setThreadLocalInfo();

        ProfileManager.ProfileElement element = manager.getProfileElement("205");
        assertNotNull(element);
        // Should fall back to the stored START_TIME string unchanged
        assertEquals("2024-01-01 08:00:00", element.toRow().get(1),
                "Should fall back to stored START_TIME string when START_TIME_MS is absent");
    }
}

