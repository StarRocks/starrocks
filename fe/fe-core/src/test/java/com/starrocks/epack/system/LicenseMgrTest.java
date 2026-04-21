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
package com.starrocks.epack.system;

import com.starrocks.common.util.MachineInfo;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.epack.persist.RegisterLicenseLog;
import com.starrocks.epack.persist.ScaleOutLicenseFreeStartTimeLog;
import com.starrocks.epack.security.SecurityUtils;
import com.starrocks.journal.JournalEntity;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LicenseMgrTest {
    private static final String SYSTEM_ID = "2f1102e0-7fc6-47fc-9f3e-c4e7e9862c18";
    private static final String AES_KEY = "x8wj$62bzp^!Kj95";
    private static final long FREE_TRIAL_EXPIRE_MS = 1000L * 3600 * 24 * 7; // 7 days
    private static final long SCALE_OUT_LICENSE_FREE_INTERVAL_MS = 1000L * 3600 * 24 * 7; // 7 days
    private static final String LICENSE_ENCRYPTED_KEY_FOR_TEST = "c2wPS3NsDnxtalt1a1Z9bmlqdXJsanJYbA9pZXNsD0tzbA90a2" +
            "pTdW5UU05uagp8ZQ1LR154S09tRVMMcnp1fWpqaXhuanlvbg95bXB6eXFsalN8bg1bc24PeW1tankNZg0GbmkNVElxa2l7cQ5uSVtTV" +
            "2p0DXVnZX5PC157W29yDVBFbmp1RXQPCwtdDnEJblRLRVtSCk1leWVKcnhldmhodVdyVAtHZWt9e2l6DmVca1dWbWwGXmlrekVmRktm" +
            "cVR5Zm5negxweGlmXGdxe3xSbUhlaWlxaw1pbltpcWdce21tblV1eWwNUEdcaXl9XQ1USWxsB0deDk90a1RtCnF3aVZtRktvaFJLd24" +
            "NT1JmD0dKaXhXXlwNeg9taVRGblIOamtVWHRweA5qZkZLaltSV0traA5LWlVXRnQOeVBqeEdualJ1cmYNC0Vlam1PdA5bbFpqeUxdan" +
            "VTaFJLdGlSdWhyanFrbQ8Oelp7ZgplaAZHZVNpSmpWS0dbblBNbmtXeV16eUdwaG4Oag1bdHJGTAxrRVNtbWt1CXB3ZQ1yegZIZWh1e" +
            "25UT0ZoeFQNaGhtdmxpUwxlamVnWlFPaGtpW2dmVXFLanlxRVxoag9mD3FMfFR5dHQNW0lsDXFFbVVTamxFZXRpDlt9XXgGaHFFV3Rr" +
            "VAoNbmhTU2oPeXBraA5OXGhLeWV3ZUltam0NbVNlcVtFfXFaUm1OclNYSHJVXEZxVXlyaQx+dF13W3Vtenltbmp2dHNsD0tzbA55a1R" +
            "uWGp5aXxrelN7dnpLeWhsD0tzbA9LfFgCAg==";

    // license1: cores: 8, expire "2025-08-01 00:00:00";       
    private static final String LICENSE1 = "BRZ7jKlHiNXFIsuy7ZhpVtWdH/bTpX0PuzR+aXNSJSfoxJl6EiBIs46cdSi5+mnTiaJCnYJ5" +
            "+o717g7zLpu15Lc3Iw+ftPyeLNAyOsu7EKVys0puoGAhmZPVp7wMIMKZfQ/LJXjbN0x1nnLj9lLXXtptVz116yTgQxy7U5hU1zfu/Vw" +
            "obex0xwHO41Q8mhOWwlbyJInastEueqW5Gi2yX7vLSZnZxlnytknH4FKTDb29bDnfDH9QFAgtXOj9eN4w7NOSBQvzOyzwH8hK4VATvV" +
            "0Z1X8266kr3OiwaHCnPiYl4aw+HGu/QgrUiF4DKzdi0ZqKwjb03nv1Vzh5i0YZg8wTBbz6OjyTpvXlyBS/cExEnvr+HGyXL9zFvc4Ox" +
            "dz+CCjSU4SIneOPTk8tCXmimc7I5Xxur8+E2f46P6iOHwPiGgdl2hiI8Zhw1iMdIXWd0e/1DB01smsACGRh24nbMbjMZ8Q4/aPnkWJM" +
            "BfKGibP9PPc4r4Up/vHOYf9z9x53DeC1rAVhhBnb4fq3FTIVIKINJ9ixnVxVZKdGaLJOSfeN4U0gO9nTSy7gmXBZx5kqfQqM7D/d9nu" +
            "Mf3v2jKr0Gw==";

    // license2: cores: 8, expire "2025-09-01 00:00:00";       
    private static final String LICENSE2 = "t+jX60BmsTJugkfP6ZYpN7oylSbbFuAyCmnUFYIY+XUdI7DF+GMoxnlJY2ICMfvMIhErgf0Z" +
            "OJ4iJK3I2XmPNTBYslMbsd0+CkIY/xskgk8RDQvXSb+c47f9T32hPybYtbU10flvt29SNr1hjY4UOQAPXcpsAdOU3/NE3piw/w33QJQ" +
            "8tlQJJhp5fY2Mp1zfHErxT9/hFqv6kXNsiIXbae5OY1NJdmE7Md8gcIeU9pINZAEymRtMqUZlfAw9FvRJVkTmp3cMqPKuH0kr6NN5KH" +
            "PUI8O8iRqgq3+V+yBgsyIgfeTRSzF7tpLNbECBlJIIX96H2azb2ZnFag6im3BBpPpP/FmSxw1OluM/mWeAiRIzug/L66wMlXnMWwF3+" +
            "+w0nRlcFPqQF86V9RTLSDiQTnGfmkKt8n86T8/cXArVkWH8u9/FXbB3ToFgutaRaqTNgOVoTEGsbjnRWldCy/RjD8w+EuvzA4pkMe8V" +
            "Cr8zfmYSBF6IcgeL00ardTk/jMxJbiSoU8p0Xkv4qgqIkgamdjDToH6bSEMQeAsDJxBr46u1IsCSIcBd2vwxhxbUyhM/2FMEChd7o2/" +
            "qh682dnNy+g==";

    private static class CreateDummyStmt extends StatementBase {
        protected CreateDummyStmt() {
            super(NodePosition.ZERO);
        }
    }

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    // verify the license encrypted by online key
    @Test
    public void testVerifyLicense() throws Exception {
        String license = "xf4+56Bv/XVizxRNpIiV0Cak28ODbZUv+JpD763Ny+fyIX03sg3NE9x93VcLLzqM2Kh7kr3w" +
                "3QfIHrdDMKnyWKvy4XSi6OlIgXVNnHRJs9RzO9XWXXNIQtKaA3Hxn6H5EXvf5KouH0anLdJpHLp5PnDcX+AFFvGNJfEvi4TNXjDP96" +
                "SY2EmMril8pulpuQWuCx/LlzR1qD8A7ZXJV3yK2dVjhU25w6oSaTdVvsguHYw1Zx16b60SnWuScykY3Z87cbkguYXz9mdQ1t3/Agcp" +
                "oek1oMK/ZhnOw435jVCHwaqhGMYb5WzhN8BO4+I3EgElSIJG7o37fwdgy6/4bNvbr3jQTP4iaDKRRz+BdHT9hQUO5a7fn5d+59/LlN" +
                "pkqXru+WUPWfihqQwztGUzsHLh2m66voMlUP3Hx8bN62XgkErHakVLwqKEHNW60OdEP641l8eLyLbeUB0ORY7ihl8Vg1DnRvCgqYuL" +
                "jtMlVWndUS+jpYnHPPkVdqvgC8tvU0NxagN5DvhzsBY10KZuGBRkmblGkHL960o5IjvLc1p5J8ACfBfI68e0Fq/JoR5nvFFHCBR+7f" +
                "x04YSO1FDT7sd7dw==";
        LicenseMgr licenseMgr = new LicenseMgr(null);
        LicenseInfo info = licenseMgr.verifyLicense(license);
        Assertions.assertNotNull(info);
        Assertions.assertEquals(6, info.getCores());
        Assertions.assertEquals(1756656000000L, info.getExpire());
    }

    @Test
    public void testAES() throws Exception {
        String data = "Hello, Celerdata";
        byte[] encryptedData = SecurityUtils.aes128Encrypt(data.getBytes(StandardCharsets.UTF_8),
                AES_KEY.getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(data,
                new String(SecurityUtils.aes128Decrypt(encryptedData, AES_KEY.getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8));
    }

    @Test
    public void testSaveLoad() throws Exception {
        String license1 = "aaaadddddddbbbbbbb";
        String license2 = "cccccccccddddddddd";
        long scaleOutLicenseFreeStartTime = 123456789L;
        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        LicenseMgr licenseMgr = new LicenseMgr(null);
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, System.currentTimeMillis());
        licenseMgr.applyInitSystemInfo(systemInfo);
        licenseMgr.applyScaleOutLicenseFreeStartTime(new ScaleOutLicenseFreeStartTimeLog(scaleOutLicenseFreeStartTime));
        licenseMgr.licenseList.add(license1);
        licenseMgr.licenseList.add(license2);
        licenseMgr.save(pseudoImage.getImageWriter());

        LicenseMgr loadedLicenseMgr = new LicenseMgr(null);
        loadedLicenseMgr.load(pseudoImage.getMetaBlockReader());
        Assertions.assertEquals(2, loadedLicenseMgr.licenseList.size());
        Assertions.assertEquals(license1, loadedLicenseMgr.licenseList.get(0));
        Assertions.assertEquals(license2, loadedLicenseMgr.licenseList.get(1));
        Assertions.assertEquals(systemInfo.getSystemID(), loadedLicenseMgr.systemInfo.getSystemID());
        Assertions.assertEquals(systemInfo.getBuildTime(), loadedLicenseMgr.systemInfo.getBuildTime());
        Assertions.assertEquals(scaleOutLicenseFreeStartTime, loadedLicenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(loadedLicenseMgr.getScaleOutStartTime());
        Assertions.assertNull(loadedLicenseMgr.getScaleOutStartTotalCpuCores());
    }

    @Test
    public void testScaleOutLicenseFreeStartTimeLogWriteAndReplay() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();

        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(10L);

        LicenseMgr leaderLicenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = leaderLicenseMgr.verifyLicense(LICENSE1);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        leaderLicenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        leaderLicenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), currentTime - FREE_TRIAL_EXPIRE_MS - 1000L));
        leaderLicenseMgr.licenseList.add(LICENSE1);

        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        leaderLicenseMgr.setScaleOutStart(scaleOutStartTime, 8L);
        leaderLicenseMgr.verifyAllLicenses();

        Assertions.assertEquals(scaleOutStartTime, leaderLicenseMgr.getScaleOutLicenseFreeStartTime());

        ScaleOutLicenseFreeStartTimeLog replayLog =
                (ScaleOutLicenseFreeStartTimeLog) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(
                        OperationTypeEPack.OP_UPDATE_SCALE_OUT_LICENSE_FREE_START_TIME);
        Assertions.assertEquals(scaleOutStartTime, replayLog.getScaleOutLicenseFreeStartTime());

        LicenseMgr followerLicenseMgr = new LicenseMgr(mockNodeMgr);
        GlobalStateMgr followerGlobalStateMgr = Mockito.mock(GlobalStateMgr.class);
        Mockito.when(followerGlobalStateMgr.getLicenseMgr()).thenReturn(followerLicenseMgr);
        GlobalStateMgr.getCurrentState().getEditLog().loadJournal(
                followerGlobalStateMgr,
                new JournalEntity(OperationTypeEPack.OP_UPDATE_SCALE_OUT_LICENSE_FREE_START_TIME, replayLog));

        Assertions.assertEquals(scaleOutStartTime, followerLicenseMgr.getScaleOutLicenseFreeStartTime());
    }

    @Test
    public void testGetRsaPubKeyObfuscation() {
        String rsaPubKey = """
                -----BEGIN PUBLIC KEY-----
                MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwiiOANNs35xthVE3g5Xi
                ynKJ7bi033arhCgntMj2h5B2I2Wbv0H9tNVqlNdK3nWQW6jANGxqhueez7dt9nwl
                +TFgJRDnIHWFv0AkNNQTLDTjO+9wslRKHHnNVGKdV2H405LrAlWufDXSTIrU7q9B
                CBOj7XzVUQLnRyESvF6M4E3aM3R1adQBCp5Hdiyxo4NjaA3kpm5o7H02o83/Ma0t
                j7D+2ee7hA7baB+lbD0TZUSKtOmUlRizNjL4Ut7buPQZxX894nLf0ECKoYmYuXsA
                jmDtON8h63Qs1837buPZo2bs3tVKvx9UpRDL+/dEJosqbBuS4A2XghIYLid2WFiV
                nwIDAQAB
                -----END PUBLIC KEY-----
                """;
        // Test that the obfuscated getRsaPubKey() method returns the same key as the original
        String obfuscatedKey = new LicenseMgr(null).getRsaPubKey().trim();
        String originalKey = rsaPubKey.trim();
        
        // Compare the keys
        Assertions.assertEquals(originalKey, obfuscatedKey, 
                "Obfuscated RSA public key should match the original key");
        
        // Also test that the key format is correct (starts with -----BEGIN PUBLIC KEY-----)
        Assertions.assertTrue(obfuscatedKey.startsWith("-----BEGIN PUBLIC KEY-----"),
                "RSA public key should start with proper header");
        Assertions.assertTrue(obfuscatedKey.endsWith("-----END PUBLIC KEY-----"),
                "RSA public key should end with proper footer");
    }

    @Test
    public void testGetAesKeyObfuscation() throws Exception {
        // Test that the obfuscated getAesKey() method returns the same key as the original
        String obfuscatedKey = LicenseMgr.getAesKey();
        String originalKey = AES_KEY;
        
        // Compare the keys
        Assertions.assertEquals(originalKey, obfuscatedKey, 
                "Obfuscated AES key should match the original key");
        
        // Test that the key length is correct (16 bytes for AES-128)
        Assertions.assertEquals(16, obfuscatedKey.length(), 
                "AES key should be 16 characters long");
    }

    @Test
    public void testFreeTrialPeriod_WithinTrial() {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        
        // Create a fixed clock set to a time within the free trial period
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS / 2; // Halfway through trial
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        // Should be in free trial period
        Assertions.assertTrue(licenseMgr.inFreeTrialPeriod(), 
                "Should be in free trial period when current time is within trial period");
    }

    @Test
    public void testFreeTrialPeriod_AfterTrial() {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        
        // Create a fixed clock set to a time after the free trial period
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS + 1000L; // After trial period
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        // Should not be in free trial period
        Assertions.assertFalse(licenseMgr.inFreeTrialPeriod(), 
                "Should not be in free trial period when current time is after trial period");
    }

    @Test
    public void testFreeTrialPeriod_AtStart() {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        
        // Create a fixed clock set to the build time (start of trial)
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime; // At start of trial
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        // Should be in free trial period
        Assertions.assertTrue(licenseMgr.inFreeTrialPeriod(), 
                "Should be in free trial period when current time equals build time");
    }

    @Test
    public void testGetAllLicenseInfo_WithFreeTrial() {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        
        // Create a fixed clock set to within the free trial period
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS / 2; // Halfway through trial
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        // Get all license info - should include free trial info
        var licenseInfos = licenseMgr.getAllLicenseInfo();
        
        // Should have one license info entry for free trial
        Assertions.assertEquals(1, licenseInfos.size(), 
                "Should have one license info entry for free trial");
        
        LicenseInfo freeTrialInfo = licenseInfos.get(0);
        Assertions.assertEquals("", freeTrialInfo.getSign(), 
                "Free trial license should have empty sign");
        Assertions.assertEquals(SYSTEM_ID, freeTrialInfo.getSystemID(), 
                "Free trial license should have correct system ID");
        Assertions.assertEquals(4L, freeTrialInfo.getCores(), 
                "Free trial license should have correct CPU cores");
        Assertions.assertEquals(buildTime + FREE_TRIAL_EXPIRE_MS, freeTrialInfo.getExpire(), 
                "Free trial license should have correct expiry time");
    }

    @Test
    public void testGetAllLicenseInfo_AfterFreeTrial() {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        
        // Create a fixed clock set to after the free trial period
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS + 1000L; // After trial period
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        // Get all license info - should not include free trial info
        var licenseInfos = licenseMgr.getAllLicenseInfo();
        
        // Should have no license info entries after free trial
        Assertions.assertEquals(0, licenseInfos.size(), 
                "Should have no license info entries after free trial period");
    }

    @Test
    public void testGetLicenseExpireDays_WithFreeTrial() {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);

        long buildTime = 1_000_000_000_000L;
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS / 2;
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);

        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        licenseMgr.applyInitSystemInfo(new SystemInfo(SYSTEM_ID, buildTime));

        long expectedDays = TimeUnit.MILLISECONDS.toDays(buildTime + FREE_TRIAL_EXPIRE_MS - currentTime);
        Assertions.assertEquals(expectedDays, licenseMgr.getLicenseExpireDays());
    }

    @Test
    public void testGetLicenseExpireDays_WithValidLicensesUsesLatestExpire() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo info1 = licenseMgr.verifyLicense(LICENSE1);
        LicenseInfo info2 = licenseMgr.verifyLicense(LICENSE2);

        long currentTime = info1.getExpire() - TimeUnit.DAYS.toMillis(1);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(info1.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.licenseList.add(LICENSE2);

        long expectedDays = TimeUnit.MILLISECONDS.toDays(info2.getExpire() - currentTime);
        Assertions.assertEquals(expectedDays, licenseMgr.getLicenseExpireDays());
    }

    @Test
    public void testVerifyAllLicenses_WithLicense1AndLicense2() throws Exception {
        // NodeMgr reports fewer cores than license allows
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);

        // Decode license details to derive systemID and expiry
        LicenseInfo info1 = licenseMgr.verifyLicense(LICENSE1);
        LicenseInfo info2 = licenseMgr.verifyLicense(LICENSE2);

        long nearBeforeExpire = Math.min(info1.getExpire(), info2.getExpire()) - 24L * 3600L * 1000L;
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(nearBeforeExpire), ZoneOffset.UTC);
        licenseMgr.setClock(fixedClock);

        // Ensure not in free trial, and systemID matches license
        long buildTime = nearBeforeExpire - FREE_TRIAL_EXPIRE_MS - 1000L;
        SystemInfo systemInfo = new SystemInfo(info1.getSystemID(), buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);

        // Add both licenses and verify
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.verifyAllLicenses();

        // After verification, license should be valid and max cores should be 8
        Assertions.assertTrue(licenseMgr.hasValidLicense());
        Assertions.assertEquals(8L, licenseMgr.getMaxCpuCores());

        // All license infos should be returned (no free trial fallback)
        var allInfos = licenseMgr.getAllLicenseInfo();
        Assertions.assertEquals(2, allInfos.size());
        Assertions.assertTrue(allInfos.stream().allMatch(li -> li.getCores() == 8L));

        Set<Long> expires = allInfos.stream().map(LicenseInfo::getExpire).collect(Collectors.toSet());
        Assertions.assertTrue(expires.contains(info1.getExpire()));
        Assertions.assertTrue(expires.contains(info2.getExpire()));
    }

    @Test
    public void testCheckLicenseForAddBackendAndFrontend_WithLicense1AndLicense2() throws Exception {
        NodeMgr backendNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(backendNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(backendNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(3L);
        Mockito.when(backendNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr backendLicenseMgr =
                new LicenseMgr(Clock.systemDefaultZone(), backendNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo backendInfo = backendLicenseMgr.verifyLicense(LICENSE1);
        long backendCurrentTime = backendInfo.getExpire() - 24L * 3600L * 1000L;
        backendLicenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(backendCurrentTime), ZoneOffset.UTC));
        long backendBuildTime = backendCurrentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        backendLicenseMgr.applyInitSystemInfo(new SystemInfo(backendInfo.getSystemID(), backendBuildTime));
        backendLicenseMgr.licenseList.add(LICENSE1);
        backendLicenseMgr.licenseList.add(LICENSE2);
        backendLicenseMgr.verifyAllLicenses();

        Assertions.assertTrue(backendLicenseMgr.checkLicenseForAddBackend("127.0.0.3"));
        Assertions.assertNull(backendLicenseMgr.getScaleOutLicenseFreeStartTime());

        NodeMgr frontendNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(frontendNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(frontendNodeMgr.getAnyFrontendCpuCores()).thenReturn(3L);
        Mockito.when(frontendNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr frontendLicenseMgr =
                new LicenseMgr(Clock.systemDefaultZone(), frontendNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo frontendInfo = frontendLicenseMgr.verifyLicense(LICENSE1);
        long frontendCurrentTime = frontendInfo.getExpire() - 24L * 3600L * 1000L;
        frontendLicenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(frontendCurrentTime), ZoneOffset.UTC));
        long frontendBuildTime = frontendCurrentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        frontendLicenseMgr.applyInitSystemInfo(new SystemInfo(frontendInfo.getSystemID(), frontendBuildTime));
        frontendLicenseMgr.licenseList.add(LICENSE1);
        frontendLicenseMgr.licenseList.add(LICENSE2);
        frontendLicenseMgr.verifyAllLicenses();

        Assertions.assertTrue(frontendLicenseMgr.checkLicenseForAddFrontend("127.0.0.3"));
        Assertions.assertNull(frontendLicenseMgr.getScaleOutLicenseFreeStartTime());
    }

    @Test
    public void testCheckLicenseForAddBackend_HostDuplicateSkipsCheck() throws Exception {
        // Configure cores so that adding nodes would exceed licensed cores (8),
        // but target host is duplicate and should be skipped
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(mockNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(3L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2",
                "127.0.0.3")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);

        // Use license data to set matching system info and a time before expiry, and after free trial
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE1);
        long currentTime = baseInfo.getExpire() - 24L * 3600L * 1000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));

        // Add both licenses and verify to activate constraints
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.verifyAllLicenses();

        Assertions.assertFalse(licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
    }

    @Test
    public void testCheckLicenseForAddFrontend_HostDuplicateSkipsCheck() throws Exception {
        // Configure cores so that adding nodes would exceed licensed cores (8),
        // but target host is duplicate and should be skipped
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(mockNodeMgr.getAnyFrontendCpuCores()).thenReturn(3L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2",
                "127.0.0.3")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);

        // Use license data to set matching system info and a time before expiry, and after free trial
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE1);
        long currentTime = baseInfo.getExpire() - 24L * 3600L * 1000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));

        // Add both licenses and verify to activate constraints
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.verifyAllLicenses();

        Assertions.assertFalse(licenseMgr.checkLicenseForAddFrontend("127.0.0.3"));
    }

    @Test
    public void testCheckLicenseForAddBackend_WithinFreeTrial() throws Exception {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        Mockito.when(mockNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(2L);
        
        // Create a fixed clock set to within the free trial period
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS / 2; // Halfway through trial
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        Assertions.assertFalse(licenseMgr.checkLicenseForAddBackend("127.0.0.3"),
                "Should not start scale-out grace when adding backend during free trial period");
    }

    @Test
    public void testCheckLicenseForAddFrontend_WithinFreeTrial() throws Exception {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);
        Mockito.when(mockNodeMgr.getAnyFrontendCpuCores()).thenReturn(2L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));
        
        // Create a fixed clock set to within the free trial period
        long buildTime = 1000000000000L; // Some build time
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS / 2; // Halfway through trial
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC);
        
        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        licenseMgr.setClock(fixedClock);
        
        
        // Initialize system info with the build time
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);
        
        Assertions.assertFalse(licenseMgr.checkLicenseForAddFrontend("127.0.0.3"),
                "Should not start scale-out grace when adding frontend during free trial period");
    }

    @Test
    public void testCheckLicenseForAddBackend_WithinScaleOutGrace_DoesNotRecordScaleOutStart() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);
        Mockito.when(mockNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(3L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long graceStartTime = currentTime - TimeUnit.DAYS.toMillis(2);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.applyScaleOutLicenseFreeStartTime(new ScaleOutLicenseFreeStartTimeLog(graceStartTime));

        Assertions.assertFalse(licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
        Assertions.assertEquals(graceStartTime, licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
    }

    @Test
    public void testCheckLicenseForAddBackend_AfterScaleOutGrace_ShouldThrow() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);
        Mockito.when(mockNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(3L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(2);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long graceStartTime = currentTime - SCALE_OUT_LICENSE_FREE_INTERVAL_MS - 1L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.applyScaleOutLicenseFreeStartTime(new ScaleOutLicenseFreeStartTimeLog(graceStartTime));

        Assertions.assertThrows(InvalidLicenseException.class,
                () -> licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
    }

    @Test
    public void testCheckLicense_AllowsCreateStatementsWithinScaleOutGrace() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long graceStartTime = currentTime - TimeUnit.DAYS.toMillis(2);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.applyScaleOutLicenseFreeStartTime(new ScaleOutLicenseFreeStartTimeLog(graceStartTime));

        licenseMgr.verifyAllLicenses();
        Assertions.assertDoesNotThrow(() -> licenseMgr.checkLicense(new CreateDummyStmt()));

        long timeAfterGraceDeadline = graceStartTime + SCALE_OUT_LICENSE_FREE_INTERVAL_MS + 1L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(timeAfterGraceDeadline), ZoneOffset.UTC));
        licenseMgr.verifyAllLicenses();
        Assertions.assertThrows(InvalidLicenseException.class, () -> licenseMgr.checkLicense(new CreateDummyStmt()));
    }

    @Test
    public void testGetLicenseExpireDays_WithScaleOutGraceUsesGraceDeadline() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long graceStartTime = currentTime - TimeUnit.DAYS.toMillis(2);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.applyScaleOutLicenseFreeStartTime(new ScaleOutLicenseFreeStartTimeLog(graceStartTime));

        long expectedDays = TimeUnit.MILLISECONDS.toDays(graceStartTime + SCALE_OUT_LICENSE_FREE_INTERVAL_MS - currentTime);
        Assertions.assertEquals(expectedDays, licenseMgr.getLicenseExpireDays());
    }

    @Test
    public void testVerifyAllLicenses_SetsScaleOutGraceAfterCpuCoresIncrease() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.verifyAllLicenses();

        licenseMgr.setScaleOutStart(scaleOutStartTime, 6L);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);

        licenseMgr.verifyAllLicenses();

        Assertions.assertEquals(scaleOutStartTime, licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTotalCpuCores());
        Assertions.assertDoesNotThrow(() -> licenseMgr.checkLicense(new CreateDummyStmt()));
    }

    @Test
    public void testVerifyAllLicenses_ClearsScaleOutStartAfterCpuCoresIncreaseWithinLicense() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE1);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.verifyAllLicenses();

        licenseMgr.setScaleOutStart(scaleOutStartTime, 4L);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);

        licenseMgr.verifyAllLicenses();

        Assertions.assertNull(licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTotalCpuCores());
    }

    @Test
    public void testVerifyAllLicenses_DoesNotStartScaleOutGraceForExpiredLicense() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() + TimeUnit.HOURS.toMillis(1);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(2);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.setScaleOutStart(scaleOutStartTime, 6L);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);

        licenseMgr.verifyAllLicenses();

        Assertions.assertNull(licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTotalCpuCores());
        Assertions.assertFalse(licenseMgr.hasValidLicense());
        Assertions.assertThrows(InvalidLicenseException.class, () -> licenseMgr.checkLicense(new CreateDummyStmt()));
    }

    @Test
    public void testVerifyAllLicenses_DoesNotStartScaleOutGraceForSystemIdMismatch() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID() + "-mismatch", buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.setScaleOutStart(scaleOutStartTime, 6L);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);

        licenseMgr.verifyAllLicenses();

        Assertions.assertNull(licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTotalCpuCores());
        Assertions.assertFalse(licenseMgr.hasValidLicense());
        Assertions.assertThrows(InvalidLicenseException.class, () -> licenseMgr.checkLicense(new CreateDummyStmt()));
    }

    @Test
    public void testVerifyAllLicenses_DoesNotStartScaleOutGraceWhenAnotherLicenseIsStillValid() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);

        long currentTime = System.currentTimeMillis();
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        long expireTime = currentTime + TimeUnit.DAYS.toMillis(30);
        String systemId = "multi-license-system-id";
        String smallLicense = "small-license";
        String largeLicense = "large-license";
        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST) {
            @Override
            protected LicenseInfo verifyLicense(String license) throws InvalidLicenseException {
                if (smallLicense.equals(license)) {
                    return new LicenseInfo("sign", systemId, 8L, expireTime);
                }
                if (largeLicense.equals(license)) {
                    return new LicenseInfo("sign", systemId, 16L, expireTime);
                }
                throw new InvalidLicenseException("Unexpected license");
            }
        };

        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(systemId, buildTime));
        licenseMgr.licenseList.add(smallLicense);
        licenseMgr.licenseList.add(largeLicense);
        licenseMgr.setScaleOutStart(scaleOutStartTime, 6L);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(12L);

        licenseMgr.verifyAllLicenses();

        Assertions.assertNull(licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTotalCpuCores());
        Assertions.assertTrue(licenseMgr.hasValidLicense());
        Assertions.assertDoesNotThrow(() -> licenseMgr.checkLicense(new CreateDummyStmt()));
    }

    @Test
    public void testCheckLicenseForAddBackend_WithPendingScaleOutStart_DoesNotRecordTwice() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE1);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.verifyAllLicenses();
        licenseMgr.setScaleOutStart(scaleOutStartTime, 6L);

        Assertions.assertFalse(licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
        Assertions.assertEquals(scaleOutStartTime, licenseMgr.getScaleOutStartTime());
        Assertions.assertEquals(6L, licenseMgr.getScaleOutStartTotalCpuCores());
    }

    @Test
    public void testCheckLicenseForAddBackend_DetectsGraceDuringVerifyAndDoesNotRecordAgain() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);
        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE2);
        long currentTime = baseInfo.getExpire() - TimeUnit.DAYS.toMillis(10);
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;
        long scaleOutStartTime = currentTime - TimeUnit.HOURS.toMillis(1);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.verifyAllLicenses();
        licenseMgr.setScaleOutStart(scaleOutStartTime, 6L);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(9L);

        Assertions.assertFalse(licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
        Assertions.assertEquals(scaleOutStartTime, licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
    }

    @Test
    public void testVerifyAllLicenses_WithLicenseExpiration() throws Exception {
        // Mock NodeMgr
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(4L);

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);

        // Get license details to understand expiration times
        LicenseInfo info1 = licenseMgr.verifyLicense(LICENSE1);
        LicenseInfo info2 = licenseMgr.verifyLicense(LICENSE2);
        
        // Ensure system info is set up to avoid free trial interference
        long buildTime = Math.min(info1.getExpire(), info2.getExpire()) - FREE_TRIAL_EXPIRE_MS - 1000L;
        SystemInfo systemInfo = new SystemInfo(info1.getSystemID(), buildTime);
        licenseMgr.applyInitSystemInfo(systemInfo);

        // Step 1: Set time before both licenses expire - both should be valid
        long timeBeforeBothExpire = Math.min(info1.getExpire(), info2.getExpire()) - 24L * 3600L * 1000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(timeBeforeBothExpire), ZoneOffset.UTC));
        
        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.licenseList.add(LICENSE2);
        licenseMgr.verifyAllLicenses();
        
        // Both licenses should be valid
        Assertions.assertTrue(licenseMgr.hasValidLicense(), 
                "Should have valid license when both licenses are active");
        Assertions.assertEquals(8L, licenseMgr.getMaxCpuCores());

        // Step 2: Set time after LICENSE1 expires but before LICENSE2 expires
        long timeAfterLicense1Expires = info1.getExpire() + 1000L; // 1 second after LICENSE1 expires
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(timeAfterLicense1Expires), ZoneOffset.UTC));
        licenseMgr.verifyAllLicenses();
        
        // LICENSE2 should still be valid
        Assertions.assertTrue(licenseMgr.hasValidLicense(), 
                "Should have valid license when LICENSE1 expires but LICENSE2 is still active");
        Assertions.assertEquals(8L, licenseMgr.getMaxCpuCores());

        // Step 3: Set time after both licenses expire
        long timeAfterBothExpire = Math.max(info1.getExpire(), info2.getExpire()) + 1000L; // 1 second after both expire
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(timeAfterBothExpire), ZoneOffset.UTC));
        licenseMgr.verifyAllLicenses();
        
        // No valid licenses should remain
        Assertions.assertFalse(licenseMgr.hasValidLicense(), 
                "Should not have valid license when both licenses have expired");
        Assertions.assertEquals(0L, licenseMgr.getMaxCpuCores());

        // Step 4: Add only LICENSE2 and verify it works independently
        licenseMgr.licenseList.clear();
        licenseMgr.licenseList.add(LICENSE2);
        
        // Set time before LICENSE2 expires
        long timeBeforeLicense2Expires = info2.getExpire() - 24L * 3600L * 1000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(timeBeforeLicense2Expires), ZoneOffset.UTC));
        licenseMgr.verifyAllLicenses();
        
        // LICENSE2 should be valid
        Assertions.assertTrue(licenseMgr.hasValidLicense(), 
                "Should have valid license when only LICENSE2 is active");
        Assertions.assertEquals(8L, licenseMgr.getMaxCpuCores());

        // Step 5: Set time after LICENSE2 expires
        long timeAfterLicense2Expires = info2.getExpire() + 1000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(timeAfterLicense2Expires), ZoneOffset.UTC));
        licenseMgr.verifyAllLicenses();
        
        // LICENSE2 should now be expired
        Assertions.assertFalse(licenseMgr.hasValidLicense(), 
                "Should not have valid license when LICENSE2 has expired");
        Assertions.assertEquals(0L, licenseMgr.getMaxCpuCores());
    }

    @Test
    public void testCheckLicenseForAddBackend_ExactlyAtLimit_NoThrow() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(mockNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(2L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);

        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE1); // cores = 8
        long currentTime = baseInfo.getExpire() - 24L * 3600L * 1000L; // before expiry
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;   // after free trial
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));

        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.verifyAllLicenses();

        Assertions.assertTrue(licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
    }

    @Test
    public void testCheckLicenseForAddFrontend_ExactlyAtLimit_NoThrow() throws Exception {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(6L);
        Mockito.when(mockNodeMgr.getAnyFrontendCpuCores()).thenReturn(2L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>(Arrays.asList("127.0.0.1", "127.0.0.2")));

        LicenseMgr licenseMgr = new LicenseMgr(Clock.systemDefaultZone(), mockNodeMgr, LICENSE_ENCRYPTED_KEY_FOR_TEST);

        LicenseInfo baseInfo = licenseMgr.verifyLicense(LICENSE1); // cores = 8
        long currentTime = baseInfo.getExpire() - 24L * 3600L * 1000L; // before expiry
        long buildTime = currentTime - FREE_TRIAL_EXPIRE_MS - 1000L;   // after free trial
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(baseInfo.getSystemID(), buildTime));

        licenseMgr.licenseList.add(LICENSE1);
        licenseMgr.verifyAllLicenses();

        Assertions.assertTrue(licenseMgr.checkLicenseForAddFrontend("127.0.0.3"));
    }

    @Test
    public void testCheckLicenseForAddBackend_NoLicense_AfterTrial_ShouldThrow() {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(2L);
        Mockito.when(mockNodeMgr.getAnyComputeNodeCpuCores()).thenReturn(1L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>());

        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);

        long buildTime = 1_000_000_000_000L;
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS + 1_000L; // after free trial
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(SYSTEM_ID, buildTime));

        // No licenses added, maxCpuCores = 0; adding would exceed and should throw
        Assertions.assertThrows(InvalidLicenseException.class,
                () -> licenseMgr.checkLicenseForAddBackend("127.0.0.3"));
    }

    @Test
    public void testCheckLicenseForAddFrontend_NoLicense_AfterTrial_ShouldThrow() {
        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getTotalCpuCores()).thenReturn(2L);
        Mockito.when(mockNodeMgr.getAnyFrontendCpuCores()).thenReturn(1L);
        Mockito.when(mockNodeMgr.getAllNodeHosts()).thenReturn(new HashSet<>());

        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);

        long buildTime = 1_000_000_000_000L;
        long currentTime = buildTime + FREE_TRIAL_EXPIRE_MS + 1_000L; // after free trial
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
        licenseMgr.applyInitSystemInfo(new SystemInfo(SYSTEM_ID, buildTime));

        // No licenses added, maxCpuCores = 0; adding would exceed and should throw
        Assertions.assertThrows(InvalidLicenseException.class,
                () -> licenseMgr.checkLicenseForAddFrontend("127.0.0.3"));
    }

    @Test
    public void testApplyRegisterLicense_ClearsScaleOutGraceStartTime() {
        LicenseMgr licenseMgr = new LicenseMgr(null);
        licenseMgr.applyScaleOutLicenseFreeStartTime(new ScaleOutLicenseFreeStartTimeLog(123456789L));
        licenseMgr.setScaleOutStart(223456789L, 16L);

        licenseMgr.applyRegisterLicense(new RegisterLicenseLog(LICENSE1, null));

        Assertions.assertEquals(1, licenseMgr.licenseList.size());
        Assertions.assertNull(licenseMgr.getScaleOutLicenseFreeStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTime());
        Assertions.assertNull(licenseMgr.getScaleOutStartTotalCpuCores());
    }


    @Test
    public void testResetSystemInfoIfMachineChanged_EmptyFeMacs_NoReset() {
        // Mock current machine MAC
        new MockUp<MachineInfo>() {
            @Mock
            public String getMacAddress() {
                return "AA-BB-CC-DD-EE-FF";
            }
        };

        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getAllFENodesMacAddress()).thenReturn(new HashSet<>());

        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        long buildTime = 1_000L;
        SystemInfo original = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(original);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(2_000L), ZoneOffset.UTC));

        licenseMgr.resetSystemInfoIfMachineChanged();

        // Should not reset when FE MAC set is empty
        Assertions.assertSame(original, licenseMgr.systemInfo);
    }

    @Test
    public void testResetSystemInfoIfMachineChanged_ContainsSelf_NoReset() {
        // Mock current machine MAC
        String mac = "11-22-33-44-55-66";
        new MockUp<MachineInfo>() {
            @Mock
            public String getMacAddress() {
                return mac;
            }
        };

        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getAllFENodesMacAddress()).thenReturn(new HashSet<>(Arrays.asList(mac, "AA-BB")));

        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        long buildTime = 1_000L;
        SystemInfo original = new SystemInfo(SYSTEM_ID, buildTime);
        licenseMgr.applyInitSystemInfo(original);
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(2_000L), ZoneOffset.UTC));

        licenseMgr.resetSystemInfoIfMachineChanged();

        // Should not reset when FE MAC set contains self MAC
        Assertions.assertSame(original, licenseMgr.systemInfo);
    }

    @Test
    public void testResetSystemInfoIfMachineChanged_NotContainsSelf_ShouldReset() {
        // Mock current machine MAC
        new MockUp<MachineInfo>() {
            @Mock
            public String getMacAddress() {
                return "AA-AA-AA-AA-AA-AA";
            }
        };

        NodeMgr mockNodeMgr = Mockito.mock(NodeMgr.class);
        Mockito.when(mockNodeMgr.getAllFENodesMacAddress()).thenReturn(new HashSet<>(Arrays.asList("BB-BB", "CC-CC")));

        LicenseMgr licenseMgr = new LicenseMgr(mockNodeMgr);
        long initialBuildTime = 1_000L;
        SystemInfo original = new SystemInfo(SYSTEM_ID, initialBuildTime);
        licenseMgr.applyInitSystemInfo(original);

        long newTime = 5_000L;
        licenseMgr.setClock(Clock.fixed(Instant.ofEpochMilli(newTime), ZoneOffset.UTC));

        licenseMgr.resetSystemInfoIfMachineChanged();

        // Should reset: new SystemInfo instance with new systemID and updated buildTime
        Assertions.assertNotSame(original, licenseMgr.systemInfo);
        Assertions.assertNotEquals(SYSTEM_ID, licenseMgr.systemInfo.getSystemID());
        Assertions.assertEquals(newTime, licenseMgr.systemInfo.getBuildTime());
    }

}
