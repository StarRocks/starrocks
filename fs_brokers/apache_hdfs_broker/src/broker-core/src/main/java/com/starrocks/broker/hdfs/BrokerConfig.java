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

package com.starrocks.broker.hdfs;

import com.starrocks.common.ConfigBase;


public class BrokerConfig extends ConfigBase {
    
    @ConfField
    public static int hdfs_read_buffer_size_kb = 8192;
    
    @ConfField
    public static int hdfs_write_buffer_size_kb = 1024;
    
    @ConfField
    public static int client_expire_seconds = 300;
    
    @ConfField
    public static int broker_ipc_port = 8000;

    /**
     * As is shown https://github.com/StarRocks/starrocks/pull/16648, broker may stuck in OSS close in some cases,
     * this will lead to all following open/write request to broker stuck.
     * To avoid this problem, we can set disable_broker_client_expiration_checking=true in apache_hdfs_broker.conf,
     * and restart broker.
     */
    @ConfField
    public static boolean disable_broker_client_expiration_checking = false;

    /**
     * If the kerberos HDFS client is alive beyond this time,
     * client checker will destroy it to avoid kerberos token expire.
     * Set this value a little smaller than the actual token expire seconds,
     * to make sure the client is destroyed before the timeout reaches.
     */
    @ConfField
    public static int kerberos_token_expire_seconds = 86000;
    
    @ConfField
    public static String sys_log_dir = System.getenv("BROKER_HOME") + "/log";
    @ConfField
    public static String sys_log_level = "INFO"; // INFO, WARNING, ERROR, FATAL
    @ConfField
    public static String sys_log_roll_mode = "SIZE-MB-1024"; // TIME-DAY
                                                             // TIME-HOUR
                                                             // SIZE-MB-nnn
    @ConfField
    public static int sys_log_roll_num = 30; // the config doesn't work if
                                             // rollmode is TIME-*
    @ConfField
    public static String audit_log_dir = System.getenv("BROKER_HOME") + "/log";
    @ConfField
    public static String[] audit_log_modules = {};
    @ConfField
    public static String audit_log_roll_mode = "TIME-DAY"; // TIME-DAY TIME-HOUR
                                                           // SIZE-MB-nnn
    @ConfField
    public static int audit_log_roll_num = 10; // the config doesn't work if
                                               // rollmode is TIME-*
    // verbose modules. VERBOSE level is implemented by log4j DEBUG level.
    @ConfField
    public static String[] sys_log_verbose_modules = { "com.starrocks" };
}
