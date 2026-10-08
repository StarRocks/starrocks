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

package com.starrocks.common;

import com.google.common.collect.Maps;
import com.starrocks.common.util.CredentialMask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class ConfigTest {
    private final Config config = new Config();

    private static class ConfigForTest extends ConfigBase {
        @ConfField(mutable = true, aliases = {"schedule_slot_num_per_path", "schedule_slot_num_per_path_only_for_test"})
        public static int tablet_sched_slot_num_per_path = 2;
    }

    @BeforeEach
    public void setUp() throws Exception {
        URL resource = getClass().getClassLoader().getResource("conf/config_test.properties");
        assert resource != null;
        config.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());
    }

    @Test
    public void testGetConfigFromPropertyFile() throws DdlException {
        PatternMatcher matcher = PatternMatcher.createMysqlPattern("tablet_sched_slot_num_per_path", false);
        List<List<String>> configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("3", configs.get(0).get(2));
    }

    @Test
    public void testConfigGetCompatibleWithOldName() throws Exception {
        URL resource = getClass().getClassLoader().getResource("conf/config_test2.properties");
        assert resource != null;
        config.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());
        PatternMatcher matcher = PatternMatcher.createMysqlPattern("schedule_slot_num_per_path", false);
        List<List<String>> configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals(1, configs.size());
        Assertions.assertEquals("3", configs.get(0).get(2));
        Assertions.assertEquals(3, Config.tablet_sched_slot_num_per_path);
        Assertions.assertEquals("tablet_sched_slot_num_per_path", configs.get(0).get(0));
        Assertions.assertTrue(configs.get(0).get(1).contains("schedule_slot_num_per_path"));
    }

    @Test
    public void testMultiAlias() throws Exception {
        ConfigForTest configForTest = new ConfigForTest();
        URL resource = getClass().getClassLoader().getResource("conf/config_test3.properties");
        assert resource != null;
        configForTest.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());
        PatternMatcher matcher = PatternMatcher.createMysqlPattern("schedule_slot_num_per_path_only_for_test", false);
        List<List<String>> configs = ConfigForTest.getConfigInfo(matcher);
        Assertions.assertEquals(1, configs.size());
        Assertions.assertEquals("5", configs.get(0).get(2));
        Assertions.assertEquals(5, ConfigForTest.tablet_sched_slot_num_per_path);
        Assertions.assertTrue(configs.get(0).get(1).contains("schedule_slot_num_per_path_only_for_test"));
    }

    @Test
    public void testConfigSetCompatibleWithOldName() throws Exception {
        Config.setMutableConfig("schedule_slot_num_per_path", "4", false, "");
        PatternMatcher matcher = PatternMatcher.createMysqlPattern("schedule_slot_num_per_path", false);
        List<List<String>> configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("4", configs.get(0).get(2));
        Assertions.assertEquals(4, Config.tablet_sched_slot_num_per_path);
    }

    @Test
    public void testMutableConfig() throws Exception {
        // Skip test if persistence is not available (container environments)
        Assumptions.assumeTrue(ConfigBase.isIsPersisted(),
                "Skipping persistence test - not available in container environment");

        PatternMatcher matcher = PatternMatcher.createMysqlPattern("adaptive_choose_instances_threshold", false);
        List<List<String>> configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("99", configs.get(0).get(2));

        PatternMatcher matcher2 = PatternMatcher.createMysqlPattern("agent_task_resend_wait_time_ms", false);
        List<List<String>> configs2 = Config.getConfigInfo(matcher2);
        Assertions.assertEquals("998", configs2.get(0).get(2));

        Config.setMutableConfig("adaptive_choose_instances_threshold", "98", true, "root");
        configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("98", configs.get(0).get(2));
        Assertions.assertEquals(98, Config.adaptive_choose_instances_threshold);

        Config.setMutableConfig("agent_task_resend_wait_time_ms", "999", true, "root");
        configs2 = Config.getConfigInfo(matcher2);
        Assertions.assertEquals("999", configs2.get(0).get(2));
        Assertions.assertEquals(999, Config.agent_task_resend_wait_time_ms);
        // Write config twice
        Config.setMutableConfig("agent_task_resend_wait_time_ms", "1000", true, "root");
        configs2 = Config.getConfigInfo(matcher2);
        Assertions.assertEquals("1000", configs2.get(0).get(2));
        Assertions.assertEquals(1000, Config.agent_task_resend_wait_time_ms);

        // Reload from file
        URL resource = getClass().getClassLoader().getResource("conf/config_test.properties");
        config.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());
        configs = Config.getConfigInfo(matcher);
        configs2 = Config.getConfigInfo(matcher2);
        Assertions.assertEquals("98", configs.get(0).get(2));
        Assertions.assertEquals("1000", configs2.get(0).get(2));
        Assertions.assertEquals(98, Config.adaptive_choose_instances_threshold);
        Assertions.assertEquals(1000, Config.agent_task_resend_wait_time_ms);
    }

    @Test
    public void testDisableStoreConfig() throws Exception {
        Config.setMutableConfig("adaptive_choose_instances_threshold", "98", false, "");
        PatternMatcher matcher = PatternMatcher.createMysqlPattern("adaptive_choose_instances_threshold", false);
        List<List<String>>  configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("98", configs.get(0).get(2));
        Assertions.assertEquals(98, Config.adaptive_choose_instances_threshold);

        // Reload from file
        URL resource = getClass().getClassLoader().getResource("conf/config_test.properties");
        config.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());
        configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("99", configs.get(0).get(2));
        Assertions.assertEquals(99, Config.adaptive_choose_instances_threshold);
    }

    private static class ConfigForArray extends ConfigBase {

        @ConfField(mutable = true)
        public static short[] prop_array_short = new short[] {1, 1};
        @ConfField(mutable = true)
        public static int[] prop_array_int = new int[] {2, 2};
        @ConfField(mutable = true)
        public static long[] prop_array_long = new long[] {3L, 3L};
        @ConfField(mutable = true)
        public static double[] prop_array_double = new double[] {1.1, 1.1};
        @ConfField(mutable = true)
        public static String[] prop_array_string = new String[] {"1", "2"};
    }

    @Test
    public void testConfigArray() throws Exception {
        ConfigForArray configForArray = new ConfigForArray();
        URL resource = getClass().getClassLoader().getResource("conf/config_test3.properties");
        assert resource != null;
        configForArray.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());
        List<List<String>> configs = ConfigForArray.getConfigInfo(null);
        Assertions.assertEquals("[1, 1]", configs.get(0).get(2));
        Assertions.assertEquals("short[]", configs.get(0).get(3));
        Assertions.assertEquals("[2, 2]", configs.get(1).get(2));
        Assertions.assertEquals("int[]", configs.get(1).get(3));
        Assertions.assertEquals("[3, 3]", configs.get(2).get(2));
        Assertions.assertEquals("long[]", configs.get(2).get(3));
        Assertions.assertEquals("[1.1, 1.1]", configs.get(3).get(2));
        Assertions.assertEquals("double[]", configs.get(3).get(3));
        Assertions.assertEquals("[1, 2]", configs.get(4).get(2));
        Assertions.assertEquals("String[]", configs.get(4).get(3));

        // check set an empty array works
        ConfigForArray.setConfigField(ConfigForArray.getAllMutableConfigs().get("prop_array_long"), "");
        configs = ConfigForArray.getConfigInfo(null);
        Assertions.assertEquals("[]", configs.get(2).get(2));
    }

    private static class ConfigForArrayDump extends ConfigBase {
        @ConfField
        public static short[] dump_array_short = new short[] {1, 2};
        @ConfField
        public static int[] dump_array_int = new int[] {3, 4};
        @ConfField
        public static long[] dump_array_long = new long[] {5L, 6L};
        @ConfField
        public static double[] dump_array_double = new double[] {1.5, 2.5};
        @ConfField
        public static boolean[] dump_array_boolean = new boolean[] {true, false};
        @ConfField
        public static String[] dump_array_string = new String[] {"a", "b"};
        @ConfField(sensitive = true)
        public static String[] dump_array_secret = new String[] {"s1", "s2"};
    }

    @Test
    public void testDumpArrayConfig() throws Exception {
        ConfigForArrayDump configForArrayDump = new ConfigForArrayDump();
        URL resource = getClass().getClassLoader().getResource("conf/config_test3.properties");
        assert resource != null;
        configForArrayDump.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());

        Map<String, String> dumped = ConfigForArrayDump.dump();
        Assertions.assertEquals("[1, 2]", dumped.get("dump_array_short"));
        Assertions.assertEquals("[3, 4]", dumped.get("dump_array_int"));
        Assertions.assertEquals("[5, 6]", dumped.get("dump_array_long"));
        Assertions.assertEquals("[1.5, 2.5]", dumped.get("dump_array_double"));
        Assertions.assertEquals("[true, false]", dumped.get("dump_array_boolean"));
        Assertions.assertEquals("[a, b]", dumped.get("dump_array_string"));
        Assertions.assertEquals(CredentialMask.LONG, dumped.get("dump_array_secret"));
    }

    private static class ConfigForSensitive extends ConfigBase {
        @ConfField(sensitive = true)
        public static String prop_secret = "wJalrXUtnFEMI/K7MDENG";
        @ConfField(sensitive = true)
        public static String prop_unset_secret = "";
        @ConfField
        public static String prop_endpoint = "http://127.0.0.1:9000";
    }

    @Test
    public void testSensitiveConfigIsMasked() throws Exception {
        ConfigForSensitive configForSensitive = new ConfigForSensitive();
        URL resource = getClass().getClassLoader().getResource("conf/config_test3.properties");
        assert resource != null;
        configForSensitive.init(Paths.get(resource.toURI()).toFile().getAbsolutePath());

        // ADMIN SHOW FRONTEND CONFIG
        Map<String, String> shown = Maps.newHashMap();
        for (List<String> row : ConfigForSensitive.getConfigInfo(null)) {
            shown.put(row.get(0), row.get(2));
        }
        Assertions.assertEquals(CredentialMask.LONG, shown.get("prop_secret"));
        // An unset credential stays empty, so it is still visible that none is configured.
        Assertions.assertEquals("", shown.get("prop_unset_secret"));
        Assertions.assertEquals("http://127.0.0.1:9000", shown.get("prop_endpoint"));

        // The /variable page
        Map<String, String> dumped = ConfigForSensitive.dump();
        Assertions.assertEquals(CredentialMask.LONG, dumped.get("prop_secret"));
        Assertions.assertEquals("", dumped.get("prop_unset_secret"));
        Assertions.assertEquals("http://127.0.0.1:9000", dumped.get("prop_endpoint"));

        // Only what is reported is masked; the config itself keeps the real value.
        Assertions.assertEquals("wJalrXUtnFEMI/K7MDENG", ConfigForSensitive.prop_secret);
    }

    @Test
    public void testCredentialConfigsAreSensitive() throws Exception {
        String[] credentials = {
                "authentication_ldap_simple_ssl_conn_trust_store_pwd",
                "authentication_ldap_simple_bind_root_pwd",
                "auth_token",
                "default_master_key",
                "aws_s3_access_key",
                "aws_s3_secret_key",
                "azure_blob_shared_key",
                "azure_blob_sas_token",
                "azure_adls2_shared_key",
                "azure_adls2_sas_token",
                "azure_adls2_oauth2_client_secret",
                "gcp_gcs_service_account_email",
                "gcp_gcs_service_account_private_key_id",
                "gcp_gcs_service_account_private_key",
                "ssl_keystore_password",
                "ssl_key_password",
                "ssl_truststore_password",
                "oauth2_client_secret",
        };
        for (String name : credentials) {
            Assertions.assertTrue(Config.class.getField(name).getAnnotation(ConfigBase.ConfField.class).sensitive(), name);
        }
        Assertions.assertFalse(Config.class.getField("aws_s3_endpoint").getAnnotation(ConfigBase.ConfField.class).sensitive());
    }
}