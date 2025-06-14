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

package com.starrocks.jni.connector;

import com.starrocks.utils.loader.ChildFirstClassLoader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ScannerHelper {
    static final String FS_OPTIONS_KV_SEPARATOR = "\u0001";
    static final String FS_OPTIONS_PROP_SEPARATOR = "\u0002";

    public static ClassLoader createChildFirstClassLoader(List<File> preloadFiles, String module) {
        URL[] jars = preloadFiles.stream().map(f -> {
            try {
                return f.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(String.format("Cannot init %s class loader.", module), e);
            }
        }).toArray(URL[]::new);
        ClassLoader classLoader = new ChildFirstClassLoader(jars, ClassLoader.getSystemClassLoader());
        return classLoader;
    }

    private static void parseKeyValuePairs(String value, String itemSep, String pairSep, Function<String[], Void> addHandler,
                                           Function<String, Void> errorHandler) {
        if (value == null) {
            return;
        }
        String[] props = value.split(itemSep);
        for (String prop : props) {
            String[] kv = prop.split(pairSep);
            if (kv.length == 2) {
                addHandler.apply(kv);
            } else {
                errorHandler.apply(prop);
            }
        }
    }

    public static void parseFSOptionsProps(String value, Function<String[], Void> addHandler,
                                           Function<String, Void> errorHandler) {
        parseKeyValuePairs(value, FS_OPTIONS_PROP_SEPARATOR, FS_OPTIONS_KV_SEPARATOR, addHandler, errorHandler);
    }

    public static void parseOptions(String value, Function<String[], Void> addHandler,
                                    Function<String, Void> errorHandler) {
        parseKeyValuePairs(value, ",", "=", addHandler, errorHandler);
    }

    public static String[] splitAndOmitEmptyStrings(String value, String separator) {
        if (value == null) {
            return new String[0];
        }
        ArrayList<String> res = new ArrayList<>();
        for (String s : value.split(separator)) {
            if (!s.equals("")) {
                res.add(s);
            }
        }
        return res.toArray(new String[0]);
    }
}
