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


package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.analyzer.SemanticException;

import java.io.StringWriter;

public class Delimiter {
    public static String convertDelimiter(String originStr) {
        if (Strings.isNullOrEmpty(originStr)) {
            throw new SemanticException("Delimiter cannot be empty or null");
        }

        if (originStr.toUpperCase().startsWith("\\X") || originStr.toUpperCase().startsWith("0X")) {
            String hexStr = originStr.substring(2);
            // check hex str
            if (hexStr.isEmpty()) {
                throw new SemanticException("Invalid delimiter '" + originStr + ": empty hex string");
            }
            if (hexStr.length() % 2 != 0) {
                throw new SemanticException("Invalid delimiter '" + originStr + ": hex length must be a even number");
            }
            for (char hexChar : hexStr.toUpperCase().toCharArray()) {
                if (ParseUtil.HEX_STRING.indexOf(hexChar) == -1) {
                    throw new SemanticException("Invalid delimiter '" + originStr + "': invalid hex format");
                }
            }

            // transform to delimiter
            StringWriter writer = new StringWriter();
            for (byte b : ParseUtil.hexStrToBytes(hexStr)) {
                writer.append((char) b);
            }
            return writer.toString();
        } else {
            return originStr;
        }
    }
}
