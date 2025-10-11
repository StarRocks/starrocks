//// Copyright 2021-present StarRocks, Inc. All rights reserved.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     https://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
//package com.starrocks.sql.parser;
//
//import com.starrocks.qe.ConnectContext;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
///**
// * SQL预处理器，在ANTLR解析之前处理大型IN谓词
// * 这是最激进的优化方案 - 完全避免ANTLR解析大量常量
// */
//public class SqlPreprocessor {
//    private static final Logger LOG = LogManager.getLogger(SqlPreprocessor.class);
//
//    // 匹配IN谓词的正则表达式，支持嵌套括号
//    private static final Pattern IN_PATTERN = Pattern.compile(
//        "(\\w+(?:\\.\\w+)?)\\s+(NOT\\s+)?IN\\s*\\(([^)]+(?:\\([^)]*\\)[^)]*)*)\\)",
//        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
//    );
//
//    // 用于替换的占位符模式
//    private static final String PLACEHOLDER_TEMPLATE = "%s %sIN (__LARGE_IN_PLACEHOLDER_%d__)";
//
//    private static final List<LargeInInfo> largeInPredicates = new ArrayList<>();
//    private static int placeholderCounter = 0;
//
//    /**
//     * 预处理SQL，将大型IN谓词替换为占位符
//     */
//    public static String preprocessSql(String originalSql) {
//        ConnectContext context = ConnectContext.get();
//        if (context == null) {
//            return originalSql;
//        }
//
//        int threshold = context.getSessionVariable().getInPredicateSemiJoinThreshold();
//
//        // 清理之前的状态
//        largeInPredicates.clear();
//        placeholderCounter = 0;
//
//        String processedSql = originalSql;
//        Matcher matcher = IN_PATTERN.matcher(originalSql);
//
//        while (matcher.find()) {
//            String column = matcher.group(1);
//            String notKeyword = matcher.group(2) != null ? matcher.group(2) : "";
//            String constantList = matcher.group(3);
//
//            // 快速估算常量数量
//            int estimatedCount = estimateConstantCount(constantList);
//
//            if (estimatedCount >= threshold) {
//                LOG.info("Preprocessing large IN predicate with ~{} constants for column {}",
//                        estimatedCount, column);
//
//                // 创建占位符
//                int placeholderId = placeholderCounter++;
//                String placeholder = String.format(PLACEHOLDER_TEMPLATE, column, notKeyword, placeholderId);
//
//                // 保存原始信息
//                largeInPredicates.add(new LargeInInfo(
//                    placeholderId, column, notKeyword.trim().length() > 0,
//                    constantList.trim(), estimatedCount
//                ));
//
//                // 替换原始SQL中的IN谓词
//                processedSql = processedSql.replace(matcher.group(0), placeholder);
//
//                LOG.debug("Replaced large IN predicate with placeholder: {}", placeholder);
//            }
//        }
//
//        if (!largeInPredicates.isEmpty()) {
//            LOG.info("Preprocessed {} large IN predicates, estimated parser speedup: {}x",
//                    largeInPredicates.size(), estimatedCount / 10);
//        }
//
//        return processedSql;
//    }
//
//    /**
//     * 快速估算常量数量（不进行完整解析）
//     */
//    private static int estimateConstantCount(String constantList) {
//        if (constantList == null || constantList.trim().isEmpty()) {
//            return 0;
//        }
//
//        // 简单的逗号计数，考虑引号内的逗号
//        int count = 1; // 至少有一个常量
//        boolean inQuotes = false;
//        char quoteChar = 0;
//
//        for (int i = 0; i < constantList.length(); i++) {
//            char c = constantList.charAt(i);
//
//            if (!inQuotes && (c == '\'' || c == '"')) {
//                inQuotes = true;
//                quoteChar = c;
//            } else if (inQuotes && c == quoteChar) {
//                // 检查是否是转义的引号
//                if (i == 0 || constantList.charAt(i - 1) != '\\') {
//                    inQuotes = false;
//                }
//            } else if (!inQuotes && c == ',') {
//                count++;
//            }
//        }
//
//        return count;
//    }
//
//    /**
//     * 获取预处理过程中提取的大型IN谓词信息
//     */
//    public static List<LargeInInfo> getLargeInPredicates() {
//        return new ArrayList<>(largeInPredicates);
//    }
//
//    /**
//     * 清理预处理状态
//     */
//    public static void cleanup() {
//        largeInPredicates.clear();
//        placeholderCounter = 0;
//    }
//
//    /**
//     * 大型IN谓词信息
//     */
//    public static class LargeInInfo {
//        public final int placeholderId;
//        public final String column;
//        public final boolean isNotIn;
//        public final String constantList;
//        public final int estimatedCount;
//
//        public LargeInInfo(int placeholderId, String column, boolean isNotIn,
//                          String constantList, int estimatedCount) {
//            this.placeholderId = placeholderId;
//            this.column = column;
//            this.isNotIn = isNotIn;
//            this.constantList = constantList;
//            this.estimatedCount = estimatedCount;
//        }
//
//        /**
//         * 推断数据类型
//         */
//        public String inferDataType() {
//            // 简单的启发式推断
//            String sample = constantList.length() > 100 ?
//                           constantList.substring(0, 100) : constantList;
//
//            if (sample.contains("'") || sample.contains("\"")) {
//                return "VARCHAR";
//            }
//
//            if (sample.contains(".")) {
//                return "DOUBLE";
//            }
//
//            // 检查是否主要是数字
//            long digitCount = sample.chars().filter(Character::isDigit).count();
//            long totalChars = sample.length();
//
//            if (digitCount > totalChars * 0.5) {
//                return "BIGINT";
//            }
//
//            return "VARCHAR";
//        }
//
//        @Override
//        public String toString() {
//            return String.format("LargeInInfo{id=%d, column='%s', isNotIn=%s, count=%d}",
//                               placeholderId, column, isNotIn, estimatedCount);
//        }
//    }
//}
