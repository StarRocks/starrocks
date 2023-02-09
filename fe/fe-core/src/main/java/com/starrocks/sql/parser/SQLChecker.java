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

package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.VariableMgr;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class SQLChecker {
    private static void checkAuditLog(String path) throws Exception {
        File auditFile = new File(path);
        if (!auditFile.exists()) {
            System.out.println("Couldn't find the fe.audit.log file");
        }

        SessionVariable sessionVariable = VariableMgr.newSessionVariable();
        sessionVariable.setSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT);
        long lineNum = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(auditFile))) {
            String line;
            StringBuilder auditBuilder = new StringBuilder();
            boolean isStart;
            while ((line = br.readLine()) != null || auditBuilder.length() != 0) {
                if (line != null) {
                    lineNum++;
                    isStart = line.contains("|Client");

                    if (!isStart) {
                        auditBuilder.append(line).append(" \n");
                        continue;
                    }

                    if (auditBuilder.length() == 0) {
                        auditBuilder.append(line).append(" \n");
                        continue;
                    }
                }

                String sql = null;
                try {
                    String auditLog = auditBuilder.toString();
                    sql = getSQL(auditLog);
                } catch (Exception e) {
                    System.out.println("AuditLog Error: " + auditBuilder);
                    continue;
                }

                try {
                    if (sql != null) {
                        com.starrocks.sql.parser.SqlParser.parse(sql, sessionVariable);
                    }
                } catch (ParsingException parsingException) {
                    System.out.println("SQL Error near line: " + lineNum + ", SQL: " + sql + ". Error: " +
                            parsingException.getMessage());
                }

                // clear audit
                auditBuilder = new StringBuilder();
                if (line != null) {
                    auditBuilder.append(line).append(" \n");
                }
            }
        }
        System.out.println("Check SQL Finish.");
    }

    private static String getSQL(String auditLog) {
        String[] strings = auditLog.split("\\|");
        if (strings.length < 14) {
            return null;
        }

        String sql = "";
        for (String string : strings) {
            if (string.contains("Stmt=")) {
                sql = string.split("Stmt=")[1].toLowerCase().trim();
                if (!sql.contains("@@")) {
                    return sql.trim();
                }
                return null;
            }
        }
        return null;
    }

    public static void main(String[] args) throws ParseException, IOException {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "help", false, "Print this usage information");
        options.addOption("f", "file", true, "Audit file path");

        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption('f')) {
            try {
                String logPath = args[1];
                if (logPath != null && !logPath.trim().startsWith("/") && !logPath.trim().startsWith("\\")) {
                    logPath = System.getProperty("user.dir") + "/" + logPath;
                }
                checkAuditLog(logPath);
            } catch (Throwable e) {
                System.out.println("Check SQL Error!");
                e.printStackTrace();
            }
        } else {
            System.out.println("usage: java -jar sqlcheck.jar -f fe.audit.log");
        }
    }
}
