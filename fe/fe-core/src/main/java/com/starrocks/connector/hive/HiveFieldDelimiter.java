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

package com.starrocks.connector.hive;

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveFieldDelimiter {
    private static final Logger LOG = LogManager.getLogger(HiveFieldDelimiter.class);

    /**
     * Convert the delimiter string configured by Hive to the actual HDFS file delimiter
     * @param originStr String
     * @return String
     */
    public static String convert(String originStr) {
        if (originStr == null || originStr.isEmpty()) {
            throw new StarRocksConnectorException("Delimiter cannot be empty or null");
        }

        // 1. Handle hexadecimal representations (\x, 0x, \X, 0X)
        if (originStr.toUpperCase().startsWith("\\X") || originStr.toUpperCase().startsWith("0X")) {
            LOG.info("origin value [{}] is use hexadecimal to format", originStr);
            return convertHexDelimiter(originStr);
        }

        // 2. Handle octal representations (\ digit, \0 digit)
        if (originStr.startsWith("\\") && originStr.length() > 1 &&
                Pattern.matches("^\\\\0?[0-7]{1,3}", originStr)) {
            LOG.info("origin value [{}] is use octal to format", originStr);
            return convertOctalDelimiter(originStr);
        }

        // 3. Handle Unicode representations (\\u, \\U)
        if (originStr.toUpperCase().startsWith("\\U")) {
            LOG.info("origin value [{}] is use unicode to format", originStr);
            return convertUnicodeDelimiter(originStr);
        }

        // 4. Handle special escape characters
        if (originStr.startsWith("\\") && originStr.length() == 2) {
            LOG.info("origin value [{}] is use special escape to format", originStr);
            return convertSpecialEscape(originStr);
        }

        // 5. Handle numeric strings (such as "1", "9", etc. representing ASCII codes)
        if (Pattern.matches("^[0-9]+$", originStr)) {
            LOG.info("origin value [{}] is use ascii code to format", originStr);
            return convertNumericDelimiter(originStr);
        }

        // 6. Default: Handle mixed escape characters
        LOG.info("origin value [{}] is use mixed escape to format", originStr);
        return convertMixedEscapedCharacters(originStr);
    }

    /**
     * Handle hexadecimal delimiters
     * @param originStr
     * @return
     */
    private static String convertHexDelimiter(String originStr) {
        String hexStr = originStr.substring(2);

        if (hexStr.isEmpty()) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': empty hex string");
        }

        // Check the hexadecimal format
        if (!Pattern.matches("^[0-9A-Fa-f]+$", hexStr)) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': invalid hex format");
        }

        // Convert hexadecimal to characters
        StringWriter writer = new StringWriter();
        try {
            // Every two characters represent one byte
            for (int i = 0; i < hexStr.length(); i += 2) {
                String byteStr = hexStr.substring(i, Math.min(i + 2, hexStr.length()));
                // If the length is odd, add zeros
                if (byteStr.length() == 1) {
                    byteStr = byteStr + "0";
                }
                int byteValue = Integer.parseInt(byteStr, 16);
                writer.append((char) byteValue);
            }
            return writer.toString();
        } catch (NumberFormatException e) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': hex conversion failed");
        }
    }

    /**
     * Handle octal delimiters
     * @param originStr
     * @return
     */
    private static String convertOctalDelimiter(String originStr) {
        // Remove the \ at the beginning
        String octalStr = originStr.substring(1);

        // Extract octal digits
        Matcher matcher = Pattern.compile("^0?([0-7]{1,3})").matcher(octalStr);
        if (matcher.find()) {
            String octalDigits = matcher.group(1);
            try {
                int octalValue = Integer.parseInt(octalDigits, 8);
                if (octalValue > 255) {
                    throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': octal value too large");
                }
                return String.valueOf((char) octalValue);
            } catch (NumberFormatException e) {
                throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': invalid octal format");
            }
        }

        throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': invalid octal format");
    }

    /**
     * Handle Unicode delimiters
     * @param originStr
     * @return
     */
    private static String convertUnicodeDelimiter(String originStr) {
        String unicodeStr = originStr.substring(2);

        if (unicodeStr.isEmpty()) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': empty unicode string");
        }

        if (!Pattern.matches("^[0-9A-Fa-f]{4,8}$", unicodeStr)) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': invalid unicode format");
        }

        try {
            int codePoint = Integer.parseInt(unicodeStr, 16);

            // Check valid Unicode code points
            if (!Character.isValidCodePoint(codePoint)) {
                throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': invalid unicode code point");
            }

            // Convert code points to strings
            return new String(Character.toChars(codePoint));
        } catch (NumberFormatException e) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': unicode conversion failed");
        }
    }

    /**
     * Handle special escape characters
     * @param originStr
     * @return
     */
    private static String convertSpecialEscape(String originStr) {
        char escapeChar = originStr.charAt(1);

        switch (escapeChar) {
            case 'a': return "\u0007";  // alert/bell
            case 'b': return "\b";      // backspace
            case 'f': return "\f";      // form feed
            case 'n': return "\n";      // newline
            case 'r': return "\r";      // carriage return
            case 't': return "\t";      // tab
            case 'v': return "\u000B";  // vertical tab
            case '\\': return "\\";     // Backslash
            case '\'': return "'";      // Single quotation marks
            case '\"': return "\"";     // Double quotation marks
            case '?': return "?";       // Question mark (used for escaping three-character groups)
            case '0': return "\0";      // Null character
            default:
                // If it's not a special escape, it might be octal or a regular character
                if (Character.isDigit(escapeChar)) {
                    // Single-digit octal numbers, such as \1, \2, etc
                    try {
                        int octalValue = Integer.parseInt(String.valueOf(escapeChar), 8);
                        return String.valueOf((char) octalValue);
                    } catch (NumberFormatException e) {
                        return String.valueOf(escapeChar);
                    }
                } else {
                    // Ordinary escape characters, such as \A, \B, etc., directly return the character
                    return String.valueOf(escapeChar);
                }
        }
    }

    /**
     * Handle numeric strings (such as "1" representing ASCII code 1)
     * @param originStr
     * @return
     */
    private static String convertNumericDelimiter(String originStr) {
        try {
            int asciiCode = Integer.parseInt(originStr);
            if (asciiCode < 0 || asciiCode > 255) {
                throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': ASCII code out of range");
            }
            return String.valueOf((char) asciiCode);
        } catch (NumberFormatException e) {
            throw new StarRocksConnectorException("Invalid delimiter '" + originStr + "': invalid numeric format");
        }
    }

    /**
     * Handle mixed escape characters (including multiple escape sequences)
     * @param input
     * @return
     */
    private static String convertMixedEscapedCharacters(String input) {
        // Use StringBuilder for more precise replacement
        StringBuilder result = new StringBuilder();
        int i = 0;

        while (i < input.length()) {
            if (input.charAt(i) == '\\' && i + 1 < input.length()) {
                // Handle escape sequences
                char nextChar = input.charAt(i + 1);

                switch (nextChar) {
                    case 'a':
                        result.append('\u0007');
                        i += 2;
                        break;
                    case 'b':
                        result.append('\b');
                        i += 2;
                        break;
                    case 'f':
                        result.append('\f');
                        i += 2;
                        break;
                    case 'n':
                        result.append('\n');
                        i += 2;
                        break;
                    case 'r':
                        result.append('\r');
                        i += 2;
                        break;
                    case 't':
                        result.append('\t');
                        i += 2;
                        break;
                    case 'v':
                        result.append('\u000B');
                        i += 2;
                        break;
                    case '\\':
                        result.append('\\');
                        i += 2;
                        break;
                    case '\'':
                        result.append('\'');
                        i += 2;
                        break;
                    case '\"':
                        result.append('\"');
                        i += 2;
                        break;
                    case '0':
                        // Check if it is an octal number
                        if (i + 2 < input.length() &&
                                Pattern.matches("[0-7]", String.valueOf(input.charAt(i + 2)))) {
                            // Handle octal escape
                            String octalStr = input.substring(i + 1, i + 4);
                            try {
                                int octalValue = Integer.parseInt(octalStr, 8);
                                result.append((char) octalValue);
                                i += 4;
                            } catch (NumberFormatException e) {
                                result.append('\0');
                                i += 2;
                            }
                        } else {
                            result.append('\0');
                            i += 2;
                        }
                        break;
                    case 'x':
                    case 'X':
                        // Handle hexadecimal escape
                        if (i + 3 < input.length()) {
                            String hexStr = input.substring(i + 2, i + 4);
                            if (Pattern.matches("[0-9A-Fa-f]{2}", hexStr)) {
                                try {
                                    int hexValue = Integer.parseInt(hexStr, 16);
                                    result.append((char) hexValue);
                                    i += 4;
                                } catch (NumberFormatException e) {
                                    result.append('\\').append(nextChar);
                                    i += 2;
                                }
                            } else {
                                result.append('\\').append(nextChar);
                                i += 2;
                            }
                        } else {
                            result.append('\\').append(nextChar);
                            i += 2;
                        }
                        break;
                    case 'u':
                    case 'U':
                        // Handle Unicode escape
                        if (i + 5 < input.length()) {
                            String unicodeStr = input.substring(i + 2, i + 6);
                            if (Pattern.matches("[0-9A-Fa-f]{4}", unicodeStr)) {
                                try {
                                    int codePoint = Integer.parseInt(unicodeStr, 16);
                                    result.append(new String(Character.toChars(codePoint)));
                                    i += 6;
                                } catch (NumberFormatException e) {
                                    result.append('\\').append(nextChar);
                                    i += 2;
                                }
                            } else {
                                result.append('\\').append(nextChar);
                                i += 2;
                            }
                        } else {
                            result.append('\\').append(nextChar);
                            i += 2;
                        }
                        break;
                    default:
                        // If it is a number, it might be octal
                        if (Character.isDigit(nextChar)) {
                            // Try to parse octal
                            int j = i + 1;
                            while (j < input.length() && j < i + 4 &&
                                    Character.isDigit(input.charAt(j)) &&
                                    input.charAt(j) >= '0' && input.charAt(j) <= '7') {
                                j++;
                            }
                            String octalStr = input.substring(i + 1, j);
                            try {
                                int octalValue = Integer.parseInt(octalStr, 8);
                                result.append((char) octalValue);
                                i = j;
                            } catch (NumberFormatException e) {
                                result.append('\\').append(nextChar);
                                i += 2;
                            }
                        } else {
                            // Ordinary escape character
                            result.append(nextChar);
                            i += 2;
                        }
                }
            } else {
                // Ordinary characters
                result.append(input.charAt(i));
                i++;
            }
        }

        return result.toString();
    }

}