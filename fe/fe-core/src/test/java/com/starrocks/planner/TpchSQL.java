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

package com.starrocks.planner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class TpchSQL {
    private static String from(String name) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/tpchsql/q" + name + ".sql");
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str).append("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SQL_MAP.put("q" + name, sb.toString());
        return sb.toString();
    }

    private static final Map<String, String> SQL_MAP = new LinkedHashMap<>();
    public static final String Q01 = from("1");
    public static final String Q02 = from("2");
    public static final String Q03 = from("3");
    public static final String Q04 = from("4");
    public static final String Q05 = from("5");
    public static final String Q06 = from("6");
    public static final String Q07 = from("7");
    public static final String Q08 = from("8");
    public static final String Q09 = from("9");
    public static final String Q10 = from("10");
    public static final String Q11 = from("11");
    public static final String Q12 = from("12");
    public static final String Q13 = from("13");
    public static final String Q14 = from("14");
    public static final String Q15 = from("15");
    public static final String Q16 = from("16");
    public static final String Q17 = from("17");
    public static final String Q18 = from("18");
    public static final String Q19 = from("19");
    public static final String Q20 = from("20");
    public static final String Q21 = from("21");
    public static final String Q22 = from("22");

    public static String getSQL(String name) {
        return SQL_MAP.get(name);
    }

    public static boolean contains(String name) {
        return SQL_MAP.containsKey(name);
    }

    public static Map<String, String> getAllSQL() {
        return SQL_MAP;
    }
}
