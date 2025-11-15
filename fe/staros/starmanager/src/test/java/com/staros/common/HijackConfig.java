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


package com.staros.common;

import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.util.Config;
import com.staros.util.ConfigBase;

import java.io.Closeable;
import java.lang.reflect.Field;

/**
 * Be able to hijack config var to a temp value and reset it to original value when needed
 */
public class HijackConfig implements Closeable {
    private Field field;
    private String originValue;

    public HijackConfig(String varName, String varValue) throws StarException {
        this.field = findField(varName);
        if (this.field == null) {
            throw new StarException(ExceptionCode.NOT_EXIST, String.format("config field %s not exist", varName));
        }

        try {
            originValue = field.get(null).toString();
            setConfigField(field, varValue);
        } catch (Exception e) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, e.getMessage());
        }
    }

    public void updateValue(String value) {
        try {
            setConfigField(field, value);
        } catch (Exception e) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, e.getMessage());
        }
    }

    private Field findField(String varName) {
        Field[] fields = new Config().getClass().getFields();
        for (Field f : fields) {
            ConfigBase.ConfField anno = f.getAnnotation(ConfigBase.ConfField.class);
            if (anno == null) {
                continue;
            }

            String confKey = anno.value().equals("") ? f.getName() : anno.value();
            if (confKey.equals(varName) || confKey.toLowerCase().equals(varName)) {
                return f;
            }
        }
        return null;
    }

    // Copy from ConfigBase
    private static void setConfigField(Field f, String confVal) throws Exception {
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

    @Override
    public void close() throws StarException {
        try {
            // reset the value
            setConfigField(field, originValue);
        } catch (Exception e) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, e.getMessage());
        }
    }

    // Alias interface to close()
    public void reset() throws StarException {
        close();
    }
}
