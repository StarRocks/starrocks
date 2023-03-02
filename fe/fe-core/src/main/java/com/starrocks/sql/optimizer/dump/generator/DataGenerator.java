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

package com.starrocks.sql.optimizer.dump.generator;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.dump.MockDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataGenerator {

    private static final Option OPTION_MODE = Option.builder("m")
            .longOpt("mode")
            .hasArg(true)
            .argName("count|gen")
            .desc("Mode, optional value can be 'count' or 'gen'. 'count' mode for printing row info" +
                    " of all tables. 'gen' mode for generating data based on table schema and column statistics.")
            .build();
    private static final Option OPTION_DUMP_PATH = Option.builder("i")
            .longOpt("path")
            .hasArg(true)
            .argName("path")
            .desc("The path of dump file in json format.")
            .build();
    private static final Option OPTION_OUTPUT_DIR = Option.builder("o")
            .longOpt("output_dir")
            .hasArg(true)
            .argName("path")
            .desc("The directory for generated data and other related files.")
            .build();
    private static final Option OPTION_NUM_ROWS = Option.builder("n")
            .longOpt("num_rows")
            .hasArg(true)
            .argName("rows")
            .desc("The maximum number of rows among all generated tables. " +
                    "For example, the rows of table t0, t1, t2 are 1000w, 100w, 10w, so t0 is the largest table. " +
                    "If we specify '-n 100000', so the ratio is 0.01, then t0 will have 10w rows, " +
                    "and the other tables will decrease proportionally, " +
                    "which means t1 has 1w rows and t2 has 1k rows. " +
                    "But if the maximum number of rows among all tables are smaller than this argument, " +
                    "then it will be simply ignored. " +
                    "Besides, for general purpose, the minimum number of generated rows is 128.")
            .build();
    private static final Option OPTION_MAX_FILE_ROWS = Option.builder()
            .longOpt("max_file_rows")
            .hasArg(true)
            .argName("rows")
            .desc("The maximum rows of each segmented csv files. If the total number of rows is greater than this, " +
                    "there will be multiply csv files with a number suffix, like 'xxx.csv-012'. " +
                    "The default value is 1000000.")
            .build();
    private static final Option OPTION_CONCURRENCY = Option.builder("c")
            .longOpt("concurrency")
            .hasArg(true)
            .argName("number")
            .desc("This argument controls how many tasks can be executed at the same time, " +
                    "each task is responsible for generating a segmented csv file. The default value is 4.")
            .build();
    private static final Option OPTION_HELP = Option.builder("h")
            .longOpt("help")
            .hasArg(false)
            .desc("Print this help document.")
            .build();

    enum Mode {
        COUNT,
        GEN;

        public static Mode of(String str) {
            for (Mode value : values()) {
                if (value.toString().toLowerCase().equals(str)) {
                    return value;
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append("mode only can be ");
            for (Mode value : values()) {
                sb.append(value.toString().toLowerCase())
                        .append("|");
            }
            sb.setLength(sb.length() - 1);
            sb.append('.');
            throw new IllegalArgumentException(sb.toString());
        }
    }

    private ExecutorService threadPool;
    private final ConnectContext session;
    private final Options options;
    private final HelpFormatter helpFormatter;
    private CommandLine commandLine;
    private Mode mode;
    private File outputDir;
    private long expectedRows;
    private long maxFileRows;
    private long maxRows;
    private QueryDumpInfo queryDumpInfo;
    private final Map<String, Long> queryDumpRowInfos = Maps.newHashMap();
    private final List<LiteralExpr> literals = Lists.newArrayList();
    private final List<TableGenerator> tableGenerators = Lists.newArrayList();

    public static void main(String[] args) {
        DataGenerator dataGenerator = new DataGenerator(args);
        dataGenerator.process();
    }

    private DataGenerator(String[] args) {
        session = new ConnectContext(null);
        session.setCurrentUserIdentity(UserIdentity.ROOT);
        session.setQualifiedUser(AuthenticationManager.ROOT_USER);
        session.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        session.setThreadLocalInfo();
        session.setDumpInfo(new MockDumpInfo());

        options = new Options();
        options.addOption(OPTION_MODE);
        options.addOption(OPTION_DUMP_PATH);
        options.addOption(OPTION_OUTPUT_DIR);
        options.addOption(OPTION_NUM_ROWS);
        options.addOption(OPTION_MAX_FILE_ROWS);
        options.addOption(OPTION_CONCURRENCY);
        options.addOption(OPTION_HELP);

        CommandLineParser parser = new DefaultParser();
        helpFormatter = new HelpFormatter();
        try {
            commandLine = parser.parse(options, args, false);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            printHelpDoc();
            System.exit(1);
        }
    }

    private void printHelpDoc() {
        helpFormatter.printHelp("\n    <1>: -m count -i <path>\n" +
                "    <2>: -m gen -i <path> -o <path> [-n <rows>] [--max_file_rows <rows>] [-c <number>]\n" +
                "    <3>: -h", options);
    }

    private void checkRequiredOption(String option) {
        Preconditions.checkState(commandLine.hasOption(option),
                String.format("Option '%s' is mandatory", option));
    }

    private void init() throws Exception {
        if (commandLine.hasOption(OPTION_HELP.getOpt())) {
            printHelpDoc();
            System.exit(0);
        }

        checkRequiredOption(OPTION_MODE.getOpt());
        mode = Mode.of(commandLine.getOptionValue(OPTION_MODE.getOpt()));

        checkRequiredOption(OPTION_DUMP_PATH.getOpt());
        File dumpFile = new File(commandLine.getOptionValue(OPTION_DUMP_PATH.getOpt()));
        Preconditions.checkState(dumpFile.exists() && dumpFile.isFile(), "dump file not exists");
        String dumpStr = IOUtils.toString(new FileInputStream(dumpFile), Charset.defaultCharset());
        queryDumpInfo = GsonUtils.GSON.fromJson(dumpStr, QueryDumpInfo.class);

        if (mode != Mode.GEN) {
            return;
        }

        // output dir
        checkRequiredOption(OPTION_OUTPUT_DIR.getOpt());
        outputDir = new File(commandLine.getOptionValue(OPTION_OUTPUT_DIR.getOpt()));
        if (!outputDir.exists()) {
            boolean res = outputDir.mkdirs();
            Preconditions.checkState(res);
        }
        Preconditions.checkState(outputDir.exists() && outputDir.isDirectory(), "output dir not exists");

        // num rows
        if (commandLine.hasOption(OPTION_NUM_ROWS.getOpt())) {
            expectedRows = Long.parseLong(commandLine.getOptionValue(OPTION_NUM_ROWS.getOpt()));
            Preconditions.checkState(expectedRows > 0, "-n <rows> must be positive");
        } else {
            expectedRows = -1;
        }

        // max file rows
        if (commandLine.hasOption(OPTION_MAX_FILE_ROWS.getLongOpt())) {
            maxFileRows = Long.parseLong(commandLine.getOptionValue(OPTION_MAX_FILE_ROWS.getLongOpt()));
            Preconditions.checkState(expectedRows > 0, "--max_file_rows <rows> must be positive");
        } else {
            maxFileRows = 1000000;
        }

        // concurrency
        int concurrency;
        if (commandLine.hasOption(OPTION_CONCURRENCY.getOpt())) {
            concurrency = Integer.parseInt(commandLine.getOptionValue(OPTION_CONCURRENCY.getOpt()));
            Preconditions.checkState(concurrency > 0, "-c <concurrency> must be positive");
            concurrency = Math.min(concurrency, Runtime.getRuntime().availableProcessors());
        } else {
            concurrency = 4;
        }
        threadPool = Executors.newFixedThreadPool(concurrency);

        File prepareFile = new File(String.format("%s/create_table.sql", outputDir.getAbsolutePath()));
        if (prepareFile.exists()) {
            boolean res = prepareFile.delete();
            Preconditions.checkState(res);
        }

        // parse query dump from json file
        String queryStmt = queryDumpInfo.getOriginStmt();
        Set<String> tableNames = Sets.newHashSet();
        for (String fullName : queryDumpInfo.getCreateTableStmtMap().keySet()) {
            String[] segments = fullName.split("\\.");
            String dbName = segments[0];
            String tableName = segments[1];
            queryStmt = queryStmt.replaceAll(String.format("%s\\.", dbName), "");
            Preconditions.checkState(tableNames.add(tableName), "duplicate table name is not allowed");
        }
        IOUtils.write(queryStmt,
                new FileOutputStream(String.format("%s/query.sql", outputDir.getAbsolutePath())),
                Charset.defaultCharset());
    }

    private void process() {
        try {
            init();
            parseRowInfo();
            if (Mode.COUNT.equals(mode)) {
                countRows();
            } else {
                generateData();
                generateShell();
            }
        } catch (UnsupportedOperationException | IllegalArgumentException | IllegalStateException e1) {
            System.err.println(e1.getMessage());
            printHelpDoc();
            System.exit(1);
        } catch (Exception e2) {
            e2.printStackTrace();
            System.exit(1);
        } finally {
            shutdown();
        }
    }

    private void shutdown() {
        if (threadPool != null && !threadPool.isShutdown()) {
            threadPool.shutdownNow();
        }
    }

    private void parseRowInfo() {
        maxRows = Long.MIN_VALUE;
        for (Map.Entry<String, String> entry : queryDumpInfo.getCreateTableStmtMap().entrySet()) {
            String fullTableName = entry.getKey();
            Map<String, Long> partitionRowCounts = queryDumpInfo.getPartitionRowCountMap().get(fullTableName);
            long rows;
            if (partitionRowCounts == null) {
                rows = 0;
            } else {
                rows = partitionRowCounts.values().stream().mapToLong(i -> i).sum();
            }
            queryDumpRowInfos.put(fullTableName, rows);
            maxRows = Math.max(maxRows, rows);
        }
    }

    private void countRows() {
        System.out.println("The number of rows from query dump file for each table are listed down below:");
        for (Map.Entry<String, String> entry : queryDumpInfo.getCreateTableStmtMap().entrySet()) {
            String fullTableName = entry.getKey();
            Long queryDumpRows = queryDumpRowInfos.get(fullTableName);
            System.out.printf("    %s: original rows=%d%n", fullTableName.split("\\.")[1],
                    queryDumpRows == null ? 0 : queryDumpRows);
        }
    }

    private void generateData() throws Exception {
        parseLiterals();

        double factor = expectedRows < 0 ? 1 : ((double) expectedRows / Math.max(maxRows, 1));
        factor = Math.min(1, factor);

        System.out.println("The number of generated rows for each table are listed down below:");
        for (Map.Entry<String, String> entry : queryDumpInfo.getCreateTableStmtMap().entrySet()) {
            String fullTableName = entry.getKey();
            String createTableSql = entry.getValue();
            CreateTableStmt stmt =
                    (CreateTableStmt) SqlParser.parse(createTableSql, session.getSessionVariable()).get(0);
            Map<String, ColumnStatistic> columnStatistics = queryDumpInfo.getTableStatisticsMap().get(fullTableName);
            Map<String, Long> partitionRowCounts = queryDumpInfo.getPartitionRowCountMap().get(fullTableName);
            TableGenerator tableGenerator = TableGenerator.create(stmt, partitionRowCounts, columnStatistics,
                    literals, factor, outputDir, maxFileRows);
            tableGenerators.add(tableGenerator);

            Long queryDumpRows = queryDumpRowInfos.get(fullTableName);
            System.out.printf("    %s: original rows=%d, generated rows=%d%n", tableGenerator.getTableName(),
                    queryDumpRows == null ? 0 : queryDumpRows, tableGenerator.getRows());
        }

        for (TableGenerator tableGenerator : tableGenerators) {
            tableGenerator.generate(threadPool);
        }
    }

    /**
     * In order to generate more relevant data, we need to parse the query and extract
     * all the literal values, and use them as candidates when generating random data.
     * For example, select v1,v2,v3 from t0 where v1 = 'city'.
     * the column v1's random generated data must contain 'city', otherwise the query has no output
     * <p>
     * For sake of simplicity, we extract all the literals, and put them together to inject to all the
     * columns only if the type is matched. Continuing with the example above, both v1 and v2 and v3 will
     * have value 'city', although only v1 is the related column.
     */
    private void parseLiterals() {
        StatementBase statementBase =
                SqlParser.parse(queryDumpInfo.getOriginStmt(), session.getSessionVariable()).get(0);

        List<LiteralExpr> literals = CollectLiteralVisitor.collect(statementBase);
        for (LiteralExpr literal : literals) {
            if (literal instanceof StringLiteral) {
                try {
                    DateLiteral dateLiteral = new DateLiteral(literal.getStringValue(), Type.DATETIME);
                    this.literals.add(dateLiteral);
                } catch (AnalysisException e1) {
                    try {
                        DateLiteral dateLiteral = new DateLiteral(literal.getStringValue(), Type.DATE);
                        this.literals.add(dateLiteral);
                    } catch (AnalysisException e2) {
                        this.literals.add(literal);
                    }
                }
            } else {
                this.literals.add(literal);
            }
        }
    }

    private void generateShell() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("#!/bin/bash\n\n");

        sb.append("set -e\n\n");

        sb.append("function echo_red () {\n" +
                "    tput setaf 1\n" +
                "    tput bold\n" +
                "    echo \"$@\"\n" +
                "    tput sgr0\n" +
                "}\n\n");
        sb.append("function echo_green () {\n" +
                "    tput setaf 2\n" +
                "    echo \"$@\"\n" +
                "    tput sgr0\n" +
                "}\n\n");

        sb.append("echo_green '============================   Loading Script Start   ============================'\n");
        sb.append("\n");

        sb.append("# check env\n");
        String[] printEnvs = {"FE_HOST", "FE_QUERY_PORT", "FE_HTTP_PORT",
                "FE_USER", "FE_PASSWD", "DATABASE", "DROP_DATABASE_IF_EXIST"};
        sb.append("echo_green 'All the related env variables are listed down below:'\n");
        for (String env : printEnvs) {
            sb.append(String.format("echo_green \"    %s='${%s}'\"\n", env, env));
        }
        sb.append("\n");
        String[] checkEnvs = {"FE_HOST", "FE_QUERY_PORT", "FE_HTTP_PORT", "FE_USER", "DATABASE"};
        for (String env : checkEnvs) {
            sb.append(String.format("if [ -z \"${%s}\" ]; then\n", env))
                    .append(String.format("    echo_red \"    Env '%s' is mandatory\"\n", env))
                    .append("    exit 1\n")
                    .append("fi\n");
        }
        sb.append("\n");

        sb.append("# create table\n");
        sb.append("echo_green 'Creating tables:'\n");
        sb.append("if [ -n \"${DROP_DATABASE_IF_EXIST}\" ]; then\n");
        sb.append("    MYSQL_PWD=${FE_PASSWD} mysql -h${FE_HOST} -P${FE_QUERY_PORT} -u${FE_USER} " +
                "-e \"DROP DATABASE IF EXISTS ${DATABASE};\"\n");
        sb.append("fi\n");
        sb.append("MYSQL_PWD=${FE_PASSWD} mysql -h${FE_HOST} -P${FE_QUERY_PORT} -u${FE_USER} " +
                "-e \"CREATE DATABASE IF NOT EXISTS ${DATABASE};\"\n");
        sb.append(String.format("MYSQL_PWD=${FE_PASSWD} mysql -h${FE_HOST} -P${FE_QUERY_PORT} " +
                "-u${FE_USER} -D${DATABASE} < %s/create_table.sql\n", outputDir.getAbsolutePath()));
        sb.append("\n");

        sb.append("# load data\n");
        sb.append("echo_green 'Loading data from csv files:'\n");
        for (int i = 0; i < tableGenerators.size(); i++) {
            TableGenerator tableGenerator = tableGenerators.get(i);
            sb.append(String.format("csv_files=( $(find %s -maxdepth 1 -name \"%s.csv-*\" | sort) )\n",
                    outputDir.getAbsolutePath(), tableGenerator.getTableName()));
            sb.append("total_loaded_rows=0\n");
            sb.append("for ((i=0; i<${#csv_files[@]}; i++))\n");
            sb.append("do\n");
            sb.append("    csv_file=${csv_files[@]:${i}:1}\n");
            sb.append("    loading_rows=$(wc -l < ${csv_file})\n");
            sb.append("    ((total_loaded_rows+=loading_rows))\n");
            sb.append(String.format("    echo_green -en \"\\r    [%d/%d] Loading table: '%s'. " +
                            "Segment<$((i+1))/${#csv_files[@]}>: '${csv_file##*/}'. " +
                            "Current loading rows: ${loading_rows}, total loaded rows: ${total_loaded_rows} ...\"\n",
                    i + 1, tableGenerators.size(), tableGenerator.getTableName()));
            sb.append(String.format("    stream_load_output=$(curl -s --location-trusted " +
                            "-u ${FE_USER}:${FE_PASSWD} -T ${csv_file} -H 'column_separator:|' " +
                            "http://${FE_HOST}:${FE_HTTP_PORT}/api/${DATABASE}/%s/_stream_load)\n",
                    tableGenerator.getTableName()));
            sb.append("    if [[ \"${stream_load_output}\" != *\"\\\"Status\\\": \\\"Success\\\"\"* ]]; then\n");
            sb.append("        echo_red -e \"    Load failed, filePath=${csv_file}, " +
                    "errorMsg:\\n${stream_load_output}\"\n");
            sb.append("        exit 1\n");
            sb.append("    fi\n");
            sb.append("done\n");
            sb.append("echo_green\n\n");
        }

        sb.append("echo_green '============================    Loading Script End    ============================'\n");

        IOUtils.write(sb.toString(),
                new FileOutputStream(String.format("%s/load_data.sh", outputDir.getAbsolutePath())),
                Charset.defaultCharset());
    }
}
