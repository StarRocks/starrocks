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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility class for parsing raw text constants into typed ConstantOperator objects.
 * This is used to convert the raw text stored in LargeInPredicate back to actual constants
 * when needed for execution or further optimization.
 */
public class RawTextConstantParser {
    private static final Logger LOG = LogManager.getLogger(RawTextConstantParser.class);
    
    // Patterns for different data types
    private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("^-?\\d+\\.\\d+$");
    private static final Pattern SCIENTIFIC_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?[eE][+-]?\\d+$");
    
    /**
     * Parse raw constant list into ConstantOperator objects based on data type hint
     */
    public static List<ConstantOperator> parseConstants(String rawConstantList, String dataTypeHint, int expectedCount) {
        if (rawConstantList == null || rawConstantList.isEmpty()) {
            return Lists.newArrayList();
        }
        
        LOG.debug("Parsing {} constants of type {} from raw text", expectedCount, dataTypeHint);
        
        // Split by comma, handling quoted strings
        List<String> tokens = splitConstantList(rawConstantList);
        
        if (tokens.size() != expectedCount) {
            LOG.warn("Expected {} constants but found {} in raw text", expectedCount, tokens.size());
        }
        
        List<ConstantOperator> constants = new ArrayList<>(tokens.size());
        
        for (String token : tokens) {
            token = token.trim();
            ConstantOperator constant = parseConstant(token, dataTypeHint);
            constants.add(constant);
        }
        
        LOG.debug("Successfully parsed {} constants", constants.size());
        return constants;
    }
    
    /**
     * Parse a single constant token based on data type hint
     */
    public static ConstantOperator parseConstant(String token, String dataTypeHint) {
        if (token == null || "NULL".equalsIgnoreCase(token)) {
            return ConstantOperator.createNull(getTypeFromHint(dataTypeHint));
        }
        
        try {
            switch (dataTypeHint.toUpperCase()) {
                case "TINYINT":
                    return ConstantOperator.createTinyInt(Byte.parseByte(token));
                    
                case "SMALLINT":
                    return ConstantOperator.createSmallInt(Short.parseShort(token));
                    
                case "INT":
                case "INTEGER":
                    return ConstantOperator.createInt(Integer.parseInt(token));
                    
                case "BIGINT":
                    return ConstantOperator.createBigint(Long.parseLong(token));
                    
                case "FLOAT":
                    return ConstantOperator.createFloat(Float.parseFloat(token));
                    
                case "DOUBLE":
                    return ConstantOperator.createDouble(Double.parseDouble(token));
                    
                case "DECIMAL":
                case "NUMERIC":
                    return ConstantOperator.createDecimal(new BigDecimal(token), getTypeFromHint(dataTypeHint));
                    
                case "BOOLEAN":
                case "BOOL":
                    return ConstantOperator.createBoolean(Boolean.parseBoolean(token));
                    
                case "DATE":
                    return ConstantOperator.createDate(parseDate(token).atStartOfDay());
                    
                case "DATETIME":
                case "TIMESTAMP":
                    return ConstantOperator.createDatetime(parseDateTime(token));
                    
                case "VARCHAR":
                case "STRING":
                case "CHAR":
                default:
                    // Remove quotes if present
                    String stringValue = removeQuotes(token);
                    return ConstantOperator.createVarchar(stringValue);
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse constant '{}' as {}, falling back to string: {}", 
                    token, dataTypeHint, e.getMessage());
            // Fallback to string
            return ConstantOperator.createVarchar(removeQuotes(token));
        }
    }
    
    /**
     * Smart parsing that infers type from the token format
     */
    public static ConstantOperator parseConstantSmart(String token) {
        if (token == null || "NULL".equalsIgnoreCase(token)) {
            return ConstantOperator.createNull(Type.VARCHAR);
        }
        
        token = token.trim();
        
        // Try boolean first
        if ("true".equalsIgnoreCase(token) || "false".equalsIgnoreCase(token)) {
            return ConstantOperator.createBoolean(Boolean.parseBoolean(token));
        }
        
        // Try integer
        if (INTEGER_PATTERN.matcher(token).matches()) {
            try {
                long value = Long.parseLong(token);
                if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                    return ConstantOperator.createInt((int) value);
                } else {
                    return ConstantOperator.createBigint(value);
                }
            } catch (NumberFormatException e) {
                // Fall through to string
            }
        }
        
        // Try decimal
        if (DECIMAL_PATTERN.matcher(token).matches() || SCIENTIFIC_PATTERN.matcher(token).matches()) {
            try {
                double value = Double.parseDouble(token);
                return ConstantOperator.createDouble(value);
            } catch (NumberFormatException e) {
                // Fall through to string
            }
        }
        
        // Default to string
        return ConstantOperator.createVarchar(removeQuotes(token));
    }
    
    /**
     * Split constant list handling quoted strings and escaped commas
     */
    private static List<String> splitConstantList(String rawList) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;
        
        for (int i = 0; i < rawList.length(); i++) {
            char c = rawList.charAt(i);
            
            if (escaped) {
                current.append(c);
                escaped = false;
                continue;
            }
            
            if (c == '\\') {
                escaped = true;
                current.append(c);
                continue;
            }
            
            if (c == '\'' || c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
                continue;
            }
            
            if (c == ',' && !inQuotes) {
                tokens.add(current.toString());
                current = new StringBuilder();
                continue;
            }
            
            current.append(c);
        }
        
        // Add the last token
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        
        return tokens;
    }
    
    /**
     * Remove surrounding quotes from string literals
     */
    private static String removeQuotes(String token) {
        if (token.length() >= 2) {
            char first = token.charAt(0);
            char last = token.charAt(token.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return token.substring(1, token.length() - 1);
            }
        }
        return token;
    }
    
    /**
     * Parse date string (YYYY-MM-DD format)
     */
    private static java.time.LocalDate parseDate(String dateStr) {
        dateStr = removeQuotes(dateStr);
        return java.time.LocalDate.parse(dateStr);
    }
    
    /**
     * Parse datetime string (YYYY-MM-DD HH:MM:SS format)
     */
    private static java.time.LocalDateTime parseDateTime(String datetimeStr) {
        datetimeStr = removeQuotes(datetimeStr);
        // Handle various datetime formats
        if (datetimeStr.contains("T")) {
            return java.time.LocalDateTime.parse(datetimeStr);
        } else {
            return java.time.LocalDateTime.parse(datetimeStr.replace(" ", "T"));
        }
    }
    
    /**
     * Get StarRocks Type from string hint
     */
    public static Type getTypeFromHint(String hint) {
        switch (hint.toUpperCase()) {
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "INT":
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "BOOLEAN":
            case "BOOL":
                return Type.BOOLEAN;
            case "DATE":
                return Type.DATE;
            case "DATETIME":
            case "TIMESTAMP":
                return Type.DATETIME;
            case "VARCHAR":
            case "STRING":
            case "CHAR":
            default:
                return Type.VARCHAR;
        }
    }
    
    /**
     * Estimate memory usage of parsed constants
     */
    public static long estimateMemoryUsage(int constantCount, String dataTypeHint) {
        int avgSize;
        switch (dataTypeHint.toUpperCase()) {
            case "TINYINT":
                avgSize = 1;
                break;
            case "SMALLINT":
                avgSize = 2;
                break;
            case "INT":
            case "FLOAT":
                avgSize = 4;
                break;
            case "BIGINT":
            case "DOUBLE":
            case "DATE":
            case "DATETIME":
                avgSize = 8;
                break;
            case "BOOLEAN":
                avgSize = 1;
                break;
            case "VARCHAR":
            case "STRING":
            default:
                avgSize = 20; // Estimate for average string
                break;
        }
        
        // Add overhead for ConstantOperator objects
        return constantCount * (avgSize + 50L);
    }
}
