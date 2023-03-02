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
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class TableGenerator {

    private final CreateTableStmt stmt;
    private final List<LiteralExpr> literals;
    private final List<ColumnGenerator<?, ?>> columnGenerators = Lists.newArrayList();
    private final long rows;
    private final long maxFileRows;
    private final File outputDir;
    private final File createTableFile;
    private final List<File> csvFiles = Lists.newArrayList();

    public static TableGenerator create(CreateTableStmt stmt, Map<String, Long> partitionRowCounts,
                                        Map<String, ColumnStatistic> columnStatistics, List<LiteralExpr> literals,
                                        double factor, File outputDir, long maxFileRows) throws Exception {
        return new TableGenerator(stmt, partitionRowCounts, columnStatistics, literals, factor, outputDir, maxFileRows);
    }

    private TableGenerator(CreateTableStmt stmt, Map<String, Long> partitionRowCounts,
                           Map<String, ColumnStatistic> columnStatistics, List<LiteralExpr> literals,
                           double factor, File outputDir, long maxFileRows) throws Exception {
        Preconditions.checkState(stmt != null);
        this.stmt = stmt;

        // For large table, we let the table size shrink with factor proportionally,
        // but for small table, we set the table size to 128 in order to generate proper output
        final long minimumRows = 128;
        if (partitionRowCounts != null) {
            long rows = partitionRowCounts.values().stream().mapToLong(i -> i).sum();
            if (rows * factor > minimumRows) {
                this.rows = (long) (rows * factor);
                Set<String> partitionNames = partitionRowCounts.keySet();
                for (String key : partitionNames) {
                    partitionRowCounts.put(key, (long) (partitionRowCounts.get(key) * factor));
                }
            } else {
                this.rows = minimumRows;
            }
        } else {
            this.rows = minimumRows;
        }

        this.maxFileRows = maxFileRows;

        this.literals = literals;
        this.outputDir = outputDir;
        this.createTableFile = new File(String.format("%s/create_table.sql", outputDir.getAbsolutePath()));

        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        // partitionName -> partitionDesc
        List<SingleRangePartitionDesc> singleRangePartitionDescs;
        String partitionColName;
        if (partitionDesc != null) {
            Preconditions.checkState(Objects.equals(PartitionType.RANGE, partitionDesc.getType()),
                    "only support range partition");
            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            Preconditions.checkState(rangePartitionDesc.getPartitionColNames().size() == 1,
                    "only support single range partition");

            partitionColName = rangePartitionDesc.getPartitionColNames().get(0);
            singleRangePartitionDescs = rangePartitionDesc.getSingleRangePartitionDescs();
        } else {
            partitionColName = null;
            singleRangePartitionDescs = null;
        }

        for (ColumnDef columnDef : stmt.getColumnDefs()) {
            ColumnStatistic columnStatistic = null;
            if (columnStatistics != null) {
                columnStatistic = columnStatistics.get(columnDef.getName());
            }
            boolean isPartitionColumn = Objects.equals(columnDef.getName(), partitionColName);
            ColumnGenerator<?, ?> columnGenerator = ColumnGenerator.create(columnDef,
                    columnStatistic, getLiteralsByType(columnDef.getType()),
                    isPartitionColumn ? singleRangePartitionDescs : null,
                    rows,
                    isPartitionColumn ? partitionRowCounts : null);
            columnGenerators.add(columnGenerator);
        }
    }

    public long getRows() {
        return rows;
    }

    public void generate(ExecutorService threads) throws Exception {
        String createTableStr = stmt.getOrigStmt().originStmt;
        createTableStr = createTableStr.replaceAll("\"dynamic_partition\\..*?\"\\s*?=\\s*?\".*?\",?\\s*?\n", "");
        IOUtils.write(createTableStr + "\n\n", new FileOutputStream(createTableFile, true), Charset.defaultCharset());

        final int taskSize = (int) Math.ceil((double) rows / maxFileRows);
        String metaFormat = String.format("%%s/%%s.csv-%%0%dd", String.valueOf(taskSize).length());
        List<Future<Void>> futures = Lists.newArrayList();

        long remainRows = rows;
        for (int i = 0; i < taskSize; i++) {
            File csvFile = new File(String.format(metaFormat, outputDir.getAbsolutePath(), stmt.getTableName(), i + 1));
            csvFiles.add(csvFile);
            long taskRows = Math.min(remainRows, maxFileRows);
            futures.add(threads.submit(new GenerateTask(columnGenerators, csvFile, taskRows)));
            remainRows -= taskRows;
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        check();
    }

    private void check() throws IOException {
        long sum = 0;
        for (File csvFile : csvFiles) {
            sum += Files.lines(csvFile.toPath()).count();
        }
        Preconditions.checkState(sum == rows, "generated row not matehes");
    }

    private List<LiteralExpr> getLiteralsByType(Type type) {
        if (type.isDateType()) {
            return literals.stream()
                    .filter(literal -> literal instanceof DateLiteral)
                    .collect(Collectors.toList());
        } else if (type.isIntegerType() || type.isLargeIntType()) {
            return literals.stream()
                    .filter(literal -> literal instanceof IntLiteral || literal instanceof LargeIntLiteral)
                    .collect(Collectors.toList());
        } else if (type.isDecimalOfAnyVersion()) {
            return literals.stream()
                    .filter(literal -> literal instanceof IntLiteral || literal instanceof LargeIntLiteral
                            || literal instanceof DecimalLiteral)
                    .collect(Collectors.toList());
        } else if (type.isFloatingPointType()) {
            return literals.stream()
                    .filter(literal -> literal instanceof IntLiteral || literal instanceof LargeIntLiteral
                            || literal instanceof DecimalLiteral || literal instanceof FloatLiteral)
                    .collect(Collectors.toList());
        } else if (type.isStringType()) {
            return Lists.newArrayList(literals);
        } else {
            return Lists.newArrayList();
        }
    }

    public String getTableName() {
        return stmt.getTableName();
    }

    private static final class GenerateTask implements Callable<Void> {
        private final List<ColumnGenerator<?, ?>> columnGenerators;
        private final File csvFile;
        private final long rows;

        public GenerateTask(List<ColumnGenerator<?, ?>> columnGenerators, File csvFile, long rows) {
            this.columnGenerators = columnGenerators;
            this.csvFile = csvFile;
            this.rows = rows;
        }

        @Override
        public Void call() throws Exception {
            try (FileOutputStream outputStream = new FileOutputStream(csvFile)) {
                for (int i = 0; i < rows; i++) {
                    StringBuilder sb = new StringBuilder();
                    for (ColumnGenerator<?, ?> columnGenerator : columnGenerators) {
                        sb.append('|');
                        Object value = columnGenerator.next();
                        if (value != null) {
                            if (value instanceof Double) {
                                sb.append(String.format("%.3f", value));
                            } else {
                                sb.append(value);
                            }
                        }
                    }
                    sb.append('\n');
                    outputStream.write(sb.substring(1).getBytes(StandardCharsets.UTF_8));

                    if (i % 100 == 0) {
                        outputStream.flush();
                    }
                }
            }
            return null;
        }
    }
}
