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

package com.starrocks.backup;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class BackupJobInfoTest {

    private static String fileName = "job_info.txt";

    private static String newFileName = "new_job_info.txt";

    @BeforeAll
    public static void createFile() {
        String json = "{\n"
                + "    \"backup_time\": 1522231864000,\n"
                + "    \"name\": \"snapshot1\",\n"
                + "    \"database\": \"db1\",\n"
                + "    \"id\": 10000,\n"
                + "    \"backup_result\": \"succeed\",\n"
                + "    \"backup_objects\": {\n"
                + "        \"table2\": {\n"
                + "            \"partitions\": {\n"
                + "                \"partition1\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"table2\": {\n"
                + "                            \"id\": 10012,\n"
                + "                            \"schema_hash\": 222222,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10030_seg1.dat\", \"__10030_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10031_seg1.dat\", \"__10031_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10011,\n"
                + "                    \"version\": 11\n"
                + "                }\n"
                + "            },\n"
                + "            \"id\": 10010\n"
                + "        },\n"
                + "        \"table1\": {\n"
                + "            \"partitions\": {\n"
                + "                \"partition2\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"rollup1\": {\n"
                + "                            \"id\": 10009,\n"
                + "                            \"schema_hash\": 333333,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10008\": [\"__10029_seg1.dat\", \"__10029_seg2.dat\"],\n"
                + "                                \"10007\": [\"__10029_seg1.dat\", \"__10029_seg2.dat\"]\n"
                + "                            }\n"
                + "                        },\n"
                + "                        \"table1\": {\n"
                + "                            \"id\": 10001,\n"
                + "                            \"schema_hash\": 444444,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10027_seg1.dat\", \"__10027_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10028_seg1.dat\", \"__10028_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10007,\n"
                + "                    \"version\": 20\n"
                + "                },\n"
                + "                \"partition1\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"rollup1\": {\n"
                + "                            \"id\": 10009,\n"
                + "                            \"schema_hash\": 333333,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10008\": [\"__10026_seg1.dat\", \"__10026_seg2.dat\"],\n"
                + "                                \"10007\": [\"__10025_seg1.dat\", \"__10025_seg2.dat\"]\n"
                + "                            }\n"
                + "                        },\n"
                + "                        \"table1\": {\n"
                + "                            \"id\": 10001,\n"
                + "                            \"schema_hash\": 444444,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10023_seg1.dat\", \"__10023_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10024_seg1.dat\", \"__10024_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10002,\n"
                + "                    \"version\": 21\n"
                + "                }\n"
                + "            },\n"
                + "            \"id\": 10001\n"
                + "        }\n"
                + "    }\n"
                + "}";

        try (PrintWriter out = new PrintWriter(fileName)) {
            out.print(json);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assertions.fail();
        }

        String newJson = "{\n"
                + "    \"backup_time\": 1522231864000,\n"
                + "    \"name\": \"snapshot1\",\n"
                + "    \"database\": \"db1\",\n"
                + "    \"id\": 10000,\n"
                + "    \"backup_result\": \"succeed\",\n"
                + "    \"backup_objects\": {\n"
                + "        \"table1\": {\n"
                + "            \"partitions\": {\n"
                + "                \"partition1\": {\n"
                + "                    \"indexes\": {\n"
                + "                        \"table1\": {\n"
                + "                            \"id\": 10001,\n"
                + "                            \"schema_hash\": 444444,\n"
                + "                            \"tablets\": {\n"
                + "                                \"10004\": [\"__10023_seg1.dat\", \"__10023_seg2.dat\"],\n"
                + "                                \"10005\": [\"__10024_seg1.dat\", \"__10024_seg2.dat\"]\n"
                + "                            }\n"
                + "                        }\n"
                + "                    },\n"
                + "                    \"id\": 10002,\n"
                + "                    \"version\": 21,\n"
                + "                    \"subPartitions\": {\n"
                + "                        \"10002\": {\n"
                + "                            \"indexes\": {\n"
                + "                                \"rollup1\": {\n"
                + "                                    \"id\": 10009,\n"
                + "                                    \"schema_hash\": 333333,\n"
                + "                                    \"tablets\": {\n"
                + "                                        \"10008\": [\"__10029_seg1.dat\", \"__10029_seg2.dat\"],\n"
                + "                                        \"10007\": [\"__10029_seg1.dat\", \"__10029_seg2.dat\"]\n"
                + "                                    }\n"
                + "                                },\n"
                + "                            },\n"
                + "                            \"id\": 10007,\n"
                + "                            \"version\": 20\n"
                + "                        }\n"
                + "                    }\n"
                + "                }\n"
                + "            },\n"
                + "            \"id\": 10001\n"
                + "        }\n"
                + "    }\n"
                + "}";

        try (PrintWriter out = new PrintWriter(newFileName)) {
            out.print(newJson);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    @AfterAll
    public static void deleteFile() {
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        File newFile = new File(fileName);
        if (newFile.exists()) {
            newFile.delete();
        }
    }
}
