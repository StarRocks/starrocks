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

import com.starrocks.epack.security.SecurityUtils;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class LicenseMgrTest {
    private static final String RSA_PUB_KEY = """
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
    private static final String LICENSE = "xf4+56Bv/XVizxRNpIiV0Cak28ODbZUv+JpD763Ny+fyIX03sg3NE9x93VcLLzqM2Kh7kr3w" +
            "3QfIHrdDMKnyWKvy4XSi6OlIgXVNnHRJs9RzO9XWXXNIQtKaA3Hxn6H5EXvf5KouH0anLdJpHLp5PnDcX+AFFvGNJfEvi4TNXjDP96" +
            "SY2EmMril8pulpuQWuCx/LlzR1qD8A7ZXJV3yK2dVjhU25w6oSaTdVvsguHYw1Zx16b60SnWuScykY3Z87cbkguYXz9mdQ1t3/Agcp" +
            "oek1oMK/ZhnOw435jVCHwaqhGMYb5WzhN8BO4+I3EgElSIJG7o37fwdgy6/4bNvbr3jQTP4iaDKRRz+BdHT9hQUO5a7fn5d+59/LlN" +
            "pkqXru+WUPWfihqQwztGUzsHLh2m66voMlUP3Hx8bN62XgkErHakVLwqKEHNW60OdEP641l8eLyLbeUB0ORY7ihl8Vg1DnRvCgqYuL" +
            "jtMlVWndUS+jpYnHPPkVdqvgC8tvU0NxagN5DvhzsBY10KZuGBRkmblGkHL960o5IjvLc1p5J8ACfBfI68e0Fq/JoR5nvFFHCBR+7f" +
            "x04YSO1FDT7sd7dw==";
    private static final String SYSTEM_ID = "2f1102e0-7fc6-47fc-9f3e-c4e7e9862c18";
    private static final String AES_KEY = "x8wj$62bzp^!Kj95";

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testVerifyLicense() throws Exception {
        LicenseInfo info = LicenseMgr.verifyLicense(LICENSE.replaceAll("\\s", ""), RSA_PUB_KEY);
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
        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        LicenseMgr licenseMgr = new LicenseMgr();
        SystemInfo systemInfo = new SystemInfo(SYSTEM_ID, System.currentTimeMillis());
        licenseMgr.applyInitSystemInfo(systemInfo);
        licenseMgr.licenseList.add(LICENSE.replaceAll("\\s", ""));
        licenseMgr.save(pseudoImage.getImageWriter());

        LicenseMgr loadedLicenseMgr = new LicenseMgr();
        loadedLicenseMgr.load(pseudoImage.getMetaBlockReader());
        Assertions.assertEquals(1, loadedLicenseMgr.licenseList.size());
        Assertions.assertEquals(LICENSE.replaceAll("\\s", ""), loadedLicenseMgr.licenseList.get(0));
        Assertions.assertEquals(systemInfo.getSystemID(), loadedLicenseMgr.systemInfo.getSystemID());
        Assertions.assertEquals(systemInfo.getBuildTime(), loadedLicenseMgr.systemInfo.getBuildTime());
    }

    @Test
    public void testGetRsaPubKeyObfuscation() throws Exception {
        // Test that the obfuscated getRsaPubKey() method returns the same key as the original
        String obfuscatedKey = LicenseMgr.getRsaPubKey().trim();
        String originalKey = RSA_PUB_KEY.trim();
        
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
}
