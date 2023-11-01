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

import com.starrocks.catalog.Table;

import java.security.SecureRandom;

public class StringUtils {
    private static final String CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static String generateRandomString(int length) {
        SecureRandom secureRandom = new SecureRandom();
        StringBuilder randomString = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int randomIndex = secureRandom.nextInt(CHARSET.length());
            char randomChar = CHARSET.charAt(randomIndex);
            randomString.append(randomChar);
        }

        return randomString.toString();
    }

    /**
     * Check two table names are equal or not.
     *
     * NOTE: OLAP table name is case-sensitive and other catalogs are not.
     */
    public static boolean areTableNamesEqual(Table table, String toCheck) {
        if (table == null) {
            return false;
        }

        if (table.isNativeTableOrMaterializedView()) {
            return table.getName().equals(toCheck);
        } else {
            return table.getName().equalsIgnoreCase(toCheck);
        }
    }

    public static boolean areColumnNamesEqual(String columName, String toCheck) {
        if (columName == null) {
            return false;
        }

        return columName.equalsIgnoreCase(toCheck);
    }
}
