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


package com.staros.util;

import java.io.FileReader;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Properties;

public class ConfigBase {
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface ConfField {
        String value() default "";

        boolean mutable() default false;

        String comment() default "";
    }

    public static Properties props;
    public static Class<? extends ConfigBase> confClass;

    public void init(String confFile) {
        try {
            props = new Properties();
            confClass = this.getClass();
            props.load(new FileReader(confFile));
            setFields();
        } catch (Exception e) {
            System.out.println("exception happen when processing config file, " + e + ".");
            System.exit(-1);
        }
    }

    private static void setFields() throws Exception {
        Field[] fields = confClass.getFields();
        for (Field f : fields) {
            // ensure that field has "@ConfField" annotation
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno == null) {
                continue;
            }

            // ensure that field has property string
            String confKey = anno.value().equals("") ? f.getName() : anno.value();
            String confVal = props.getProperty(confKey.toLowerCase());
            if (confVal == null || confVal.isEmpty()) {
                continue;
            }

            setConfigField(f, confVal);
        }
    }

    private static void setConfigField(Field f, String confVal) throws IllegalAccessException, Exception {
        confVal = confVal.trim();

        String[] sa = confVal.split(",");
        for (int i = 0; i < sa.length; i++) {
            sa[i] = sa[i].trim();
        }

        // set config field
        switch (f.getType().getSimpleName()) {
            case "short":
                f.setShort(null, Short.parseShort(confVal));
                break;
            case "int":
                f.setInt(null, Integer.parseInt(confVal));
                break;
            case "long":
                f.setLong(null, Long.parseLong(confVal));
                break;
            case "double":
                f.setDouble(null, Double.parseDouble(confVal));
                break;
            case "boolean":
                f.setBoolean(null, Boolean.parseBoolean(confVal));
                break;
            case "String":
                f.set(null, confVal);
                break;
            case "short[]":
                short[] sha = new short[sa.length];
                for (int i = 0; i < sha.length; i++) {
                    sha[i] = Short.parseShort(sa[i]);
                }
                f.set(null, sha);
                break;
            case "int[]":
                int[] ia = new int[sa.length];
                for (int i = 0; i < ia.length; i++) {
                    ia[i] = Integer.parseInt(sa[i]);
                }
                f.set(null, ia);
                break;
            case "long[]":
                long[] la = new long[sa.length];
                for (int i = 0; i < la.length; i++) {
                    la[i] = Long.parseLong(sa[i]);
                }
                f.set(null, la);
                break;
            case "double[]":
                double[] da = new double[sa.length];
                for (int i = 0; i < da.length; i++) {
                    da[i] = Double.parseDouble(sa[i]);
                }
                f.set(null, da);
                break;
            case "boolean[]":
                boolean[] ba = new boolean[sa.length];
                for (int i = 0; i < ba.length; i++) {
                    ba[i] = Boolean.parseBoolean(sa[i]);
                }
                f.set(null, ba);
                break;
            case "String[]":
                f.set(null, sa);
                break;
            default:
                throw new Exception("unknown type: " + f.getType().getSimpleName());
        }
    }
}
