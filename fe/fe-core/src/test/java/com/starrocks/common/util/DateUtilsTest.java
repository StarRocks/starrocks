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

import com.starrocks.sql.optimizer.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class DateUtilsTest {

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
                String datetime4 = "20250225112345";
                LocalDateTime lt4 = DateUtils.parseStrictDateTime(datetime4);
                Assert.assertEquals(lt4.toString(), "2025-02-25T11:23:45");
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testProbeFormat() {
        try {
            String datetime = "20250225112345";
            DateTimeFormatter dateTimeFormatter = DateUtils.probeFormat(datetime);
            Assert.assertEquals(dateTimeFormatter, DateUtils.DATE_TIME_S_FORMATTER_UNIX);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testParseStringWithDefaultHSM() {
        try {
            String datetime1 = "20250225112345";
            LocalDateTime localDateTime1 =
                    DateUtils.parseStringWithDefaultHSM(datetime1, DateUtils.probeFormat(datetime1));
            Assert.assertTrue(localDateTime1.getYear() == 2025 && localDateTime1.getMonthValue() == 2 &&
                    localDateTime1.getDayOfMonth() == 25
                    && localDateTime1.getHour() == 11 && localDateTime1.getMinute() == 23 &&
                    localDateTime1.getSecond() == 45);

            String datetime2 = "2025-02-25 11:23:45";
            LocalDateTime localDateTime2 =
                    DateUtils.parseStringWithDefaultHSM(datetime2, DateUtils.probeFormat(datetime2));
            Assert.assertTrue(localDateTime2.getYear() == 2025 && localDateTime2.getMonthValue() == 2 &&
                    localDateTime2.getDayOfMonth() == 25
                    && localDateTime2.getHour() == 11 && localDateTime2.getMinute() == 23 &&
                    localDateTime2.getSecond() == 45);

            String datetime3 = "2025-02-25";
            LocalDateTime localDateTime3 =
                    DateUtils.parseStringWithDefaultHSM(datetime3, DateUtils.probeFormat(datetime3));
            Assert.assertTrue(localDateTime3.getYear() == 2025 && localDateTime3.getMonthValue() == 2 &&
                    localDateTime3.getDayOfMonth() == 25
                    && localDateTime3.getHour() == 0 && localDateTime3.getMinute() == 0 &&
                    localDateTime3.getSecond() == 0);

        } catch (Exception e) {
            Assert.fail();
        }
    }

}
