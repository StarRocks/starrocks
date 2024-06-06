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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/util/BrokerUtilTest.java

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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.system.Backend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateUtilsTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setIsDecommissioned(true);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        Config.enable_strict_storage_medium_check = true;
        Config.enable_auto_tablet_distribution = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        // set time_zone
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setTimeZone("Asia/Shanghai");
        ConnectContext.get().setSessionVariable(sessionVariable);
    }

    @Test
    public void testParseDatTimeString() {
        try {
            {
                String datetime1 = "2023-04-27 21:06:11";
                Assert.assertThrows(DateTimeParseException.class,
                        () -> DateUtils.parseStringWithDefaultHSM(datetime1, DateUtils.DATE_TIME_MS_FORMATTER_UNIX));
                LocalDateTime lt1 = DateUtils.parseStringWithDefaultHSM(datetime1, DateUtils.DATE_TIME_FORMATTER_UNIX);
                LocalDateTime lt2 = DateUtils.parseDatTimeString(datetime1);
                Assert.assertEquals(lt1, lt2);
                long ts = Utils.getLongFromDateTime(lt2);
                Assert.assertEquals(ts, 1682600771);
            }

            {
                String datetime1 = "2023-04-27 21:06:11.108000";
                Assert.assertThrows(DateTimeParseException.class,
                        () -> DateUtils.parseStringWithDefaultHSM(datetime1, DateUtils.DATE_TIME_FORMATTER_UNIX));
                LocalDateTime lt1 = DateUtils.parseStringWithDefaultHSM(datetime1, DateUtils.DATE_TIME_MS_FORMATTER_UNIX);
                LocalDateTime lt2 = DateUtils.parseDatTimeString(datetime1);
                Assert.assertEquals(lt1, lt2);
                long ts = Utils.getLongFromDateTime(lt2);
                Assert.assertEquals(ts, 1682600771);
            }

            {
                String datetime1 = "2024-01-27T21:06";
                LocalDateTime lt1 = DateUtils.parseStrictDateTime(datetime1);
                Assert.assertEquals(lt1.toString(), "2024-01-27T21:06");
                String datetime2 = "2024-01-27T21:06:00";
                LocalDateTime lt2 = DateUtils.parseStrictDateTime(datetime2);
                Assert.assertEquals(lt2.toString(), "2024-01-27T21:06");
                Assert.assertEquals(Utils.getLongFromDateTime(lt1), Utils.getLongFromDateTime(lt2));
                String datetime3 = "2024-01-27 21:06:01";
                LocalDateTime lt3 = DateUtils.parseStrictDateTime(datetime3);
                Assert.assertEquals(lt3.toString(), "2024-01-27T21:06:01");
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testParseDateTimeWithTimezone() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter dtfMs = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
        try {
            {
                String datetime = "2020-01-01 00:00:00-00:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 08:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00-01:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 09:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00-08:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 16:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00-12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 20:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00+00:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 08:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00+01:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 07:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00+08:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01 00:00:00", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01 00:00:00+12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2019-12-31 20:00:00", localDateTime.format(dtf));
            }
            // with milliseconds
            {
                String datetime = "2020-01-01 00:00:00.123456+12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2019-12-31 20:00:00.123456", localDateTime.format(dtfMs));
            }
            // with spaces
            {
                String datetime = "2020-01-01 00:00:00.123456    +12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2019-12-31 20:00:00.123456", localDateTime.format(dtfMs));
            }
            {
                String datetime = "2020-01-01 00:00:00.123456    +12:00    ";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2019-12-31 20:00:00.123456", localDateTime.format(dtfMs));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testParseDateWithTimezone() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try {
            {
                String datetime = "2020-01-01-00:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01-12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01+00:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01+12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01", localDateTime.format(dtf));
            }
            // with spaces
            {
                String datetime = "2020-01-01    +12:00";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01", localDateTime.format(dtf));
            }
            {
                String datetime = "2020-01-01    +12:00    ";
                LocalDateTime localDateTime = DateUtils.parseStrictDateTime(datetime);
                Assert.assertEquals("2020-01-01", localDateTime.format(dtf));
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }
}
