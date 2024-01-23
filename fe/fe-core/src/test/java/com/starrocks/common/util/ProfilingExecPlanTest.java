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

import com.google.common.collect.Sets;
import com.starrocks.planner.PlanNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ProfilingExecPlanTest {
    public static List<Class<?>> findClassesExtending(Class<?> superClass, String packageName)
            throws ClassNotFoundException {
        List<Class<?>> classes = new ArrayList<>();
        String path = packageName.replace('.', File.separatorChar);
        String[] classPathEntries = System.getProperty("java.class.path").split(System.getProperty("path.separator"));

        for (String classPathEntry : classPathEntries) {
            File baseDir = new File(classPathEntry + File.separatorChar + path);

            if (baseDir.isDirectory()) {
                findClassesExtendingInDirectory(baseDir, superClass, packageName, classes);
            }
        }

        return classes;
    }

    private static void findClassesExtendingInDirectory(File directory, Class<?> superClass, String packageName,
                                                        List<Class<?>> classes) throws ClassNotFoundException {
        File[] files = directory.listFiles();

        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isDirectory()) {
                findClassesExtendingInDirectory(file, superClass, packageName + "." + file.getName(), classes);
            } else if (file.getName().endsWith(".class")) {
                String className = packageName + "." + file.getName().substring(0, file.getName().length() - 6);
                Class<?> clazz = Class.forName(className);

                if (superClass.isAssignableFrom(clazz) && !superClass.equals(clazz)) {
                    classes.add(clazz);
                }
            }
        }
    }

    @Test
    public void testNormalizeName() throws Exception {
        Set<String> names =
                Sets.newHashSet("REPEAT", "UNION", "HDFS_SCAN", "EXCEPT", "NEST_LOOP_JOIN", "DELTA_LAKE_SCAN",
                        "MERGE_JOIN", "FILE_TABLE_SCAN", "JDBC_SCAN", "DECODE", "BINLOG_SCAN", "INTERSECT", "SORT",
                        "STREAM_AGG", "STREAM_JOIN", "PROJECT", "PAIMON_SCAN", "TABLE_FUNCTION", "MYSQL_SCAN",
                        "EMPTY_SET", "HUDI_SCAN", "HASH_JOIN", "ES_SCAN", "SCHEMA_SCAN", "ASSERT_NUM_ROWS", "SELECT",
                        "STREAM_LOAD_SCAN", "ANALYTIC_EVAL", "ICEBERG_SCAN", "AGGREGATION", "FILE_SCAN", "EXCHANGE",
                        "META_SCAN", "OLAP_SCAN", "ODPS_SCAN");

        Method method = ProfilingExecPlan.class.getDeclaredMethod("normalizeNodeName", Class.class);
        method.setAccessible(true);
        List<Class<?>> classesExtending = findClassesExtending(PlanNode.class, "com.starrocks.planner");
        for (Class<?> aClass : classesExtending) {
            if (Modifier.isAbstract(aClass.getModifiers())) {
                continue;
            }
            Assert.assertTrue(names.contains((String) method.invoke(null, aClass)));
        }
    }
}
