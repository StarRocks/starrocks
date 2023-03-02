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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.ScalarType;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import org.apache.commons.lang3.tuple.Triple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ColumnGenerator<Type, GenType> {
    private static final AtomicLong SEED = new AtomicLong();
    protected final Random random = new Random(SEED.incrementAndGet());
    protected final ColumnDef columnDef;
    protected final ColumnStatistic columnStatistic;
    protected final List<LiteralExpr> literalExprs;
    protected final LinkedList<GenType> literals = Lists.newLinkedList();
    protected Type typeMin;
    protected Type typeMax;
    private final long rows;
    private final Set<GenType> distinctValueSet = Sets.newHashSet();
    private List<GenType> distinctValueList = null;
    private boolean isInit = false;

    protected ColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic, List<LiteralExpr> literalExprs,
                              long rows) {
        this.columnDef = columnDef;
        this.columnStatistic = columnStatistic;
        this.literalExprs = literalExprs;
        this.rows = rows;
    }

    /**
     * This method itself never becomes the bottleneck of performance when generating data
     * So the simplest way to support concurrent is by adding synchronized modifier
     */
    public final synchronized GenType next() throws Exception {
        if (!isInit) {
            init();
            isInit = true;
        }

        if (nextNull()) {
            return null;
        } else if (distinctValueList != null) {
            return distinctValueList.get(random.nextInt(distinctValueList.size()));
        } else if (!literals.isEmpty()) {
            GenType next = literals.pop();
            distinctValueSet.add(next);
            return next;
        } else if (!isStatisticsValid()) {
            GenType next = nextDefault();
            distinctValueSet.add(next);
            int distinctValuesCount;
            if (rows < 1000) {
                distinctValuesCount = 16;
            } else if (rows < 1000000) {
                distinctValuesCount = 256;
            } else {
                distinctValuesCount = 1024;
            }
            if (distinctValueSet.size() >= distinctValuesCount) {
                distinctValueList = Lists.newArrayList(distinctValueSet);
                distinctValueSet.clear();
            }
            return next;
        } else {
            GenType next = nextWithStatistics();
            distinctValueSet.add(next);
            if (distinctValueSet.size() >= columnStatistic.getDistinctValuesCount()) {
                distinctValueList = Lists.newArrayList(distinctValueSet);
                distinctValueSet.clear();
            }
            return next;
        }
    }

    private void init() {
        Collections.shuffle(this.literals);
    }

    private boolean nextNull() {
        if (columnStatistic != null && columnDef.isAllowNull()) {
            return random.nextDouble() < columnStatistic.getNullsFraction();
        } else {
            return false;
        }
    }

    protected abstract GenType nextDefault() throws Exception;

    protected abstract GenType nextWithStatistics() throws Exception;

    protected final boolean isStatisticsValid() {
        return columnStatistic != null
                && !Double.isNaN(columnStatistic.getMinValue())
                && !Double.isInfinite(columnStatistic.getMinValue())
                && Double.isFinite(columnStatistic.getMinValue())
                && !Double.isNaN(columnStatistic.getMaxValue())
                && !Double.isInfinite(columnStatistic.getMaxValue())
                && Double.isFinite(columnStatistic.getMaxValue());
    }

    protected final double intervalRandom(Number begin, Number end) {
        return Math.round(begin.doubleValue() + random.nextDouble() * (end.doubleValue() - begin.doubleValue()));
    }

    protected final long intervalRandomForLong(Long begin, Long end) {
        if (begin < end && end - begin <= Integer.MAX_VALUE) {
            return begin + random.nextInt((int) (end - begin));
        }
        return (long) intervalRandom(begin, end);
    }

    public static ColumnGenerator<?, ?> create(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                               List<LiteralExpr> literalExprs,
                                               List<SingleRangePartitionDesc> singleRangePartitionDescs, long rows,
                                               Map<String, Long> partitionRowCounts) throws Exception {
        if (columnDef.getType().isBoolean()) {
            return new BooleanColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else if (columnDef.getType().isIntegerType()) {
            return new IntegerColumnGenerator(columnDef, columnStatistic, literalExprs, singleRangePartitionDescs, rows,
                    partitionRowCounts);
        } else if (columnDef.getType().isLargeIntType()) {
            return new LargeIntColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else if (columnDef.getType().isFloatingPointType()) {
            return new FloatColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else if (columnDef.getType().isDecimalOfAnyVersion()) {
            return new DecimalColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else if (columnDef.getType().isDateType()) {
            return new TimeColumnGenerator(columnDef, columnStatistic, literalExprs, singleRangePartitionDescs, rows,
                    partitionRowCounts);
        } else if (columnDef.getType().isStringType()) {
            return new StringColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else if (columnDef.getType().isArrayType()) {
            return new ArrayColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else if (columnDef.getType().isJsonType()) {
            return new JsonColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        } else {
            return new NullColumnGenerator(columnDef, columnStatistic, literalExprs, rows);
        }
    }

    private abstract static class PartitionColumnGenerator<GenType> extends ColumnGenerator<Long, GenType> {
        protected final List<SingleRangePartitionDesc> singleRangePartitionDescs;
        protected final Map<String, Long> partitionRowNums;

        protected final List<Triple<Long, Long, PartitionKeyDesc.PartitionRangeType>> partitionBoundaries =
                Lists.newArrayList();
        private boolean partitionFinished = true;
        private int partitionIndex = 0;
        private long partitionRowNum = 0;
        private long partitionCnt = 0;

        public PartitionColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                        List<LiteralExpr> literalExprs,
                                        List<SingleRangePartitionDesc> singleRangePartitionDescs, long rows,
                                        Map<String, Long> partitionRowNums) throws Exception {
            super(columnDef, columnStatistic, literalExprs, rows);

            this.singleRangePartitionDescs = singleRangePartitionDescs;
            this.partitionRowNums = partitionRowNums;

            initTypeInfo();
        }

        protected abstract void initTypeInfo() throws Exception;

        protected final Long nextWithPartitionInfo() {
            if (singleRangePartitionDescs != null && partitionRowNums != null) {
                findNextPartitionIdx();
                if (partitionIndex < singleRangePartitionDescs.size()) {
                    Triple<Long, Long, PartitionKeyDesc.PartitionRangeType> boundary =
                            partitionBoundaries.get(partitionIndex);
                    boolean isFixed = PartitionKeyDesc.PartitionRangeType.FIXED.equals(boundary.getRight());
                    long next = intervalRandomForLong(boundary.getLeft() + (isFixed ? 0 : 1), boundary.getMiddle());
                    if (++partitionCnt >= partitionRowNum) {
                        partitionIndex++;
                        partitionFinished = true;
                    }
                    return next;
                } else {
                    return intervalRandomForLong(typeMin, typeMax);
                }
            }
            return null;
        }

        private void findNextPartitionIdx() {
            if (partitionIndex >= singleRangePartitionDescs.size()) {
                return;
            }
            if (!partitionFinished) {
                return;
            }
            partitionCnt = 0;
            while (partitionIndex < singleRangePartitionDescs.size()) {
                SingleRangePartitionDesc singleRangePartitionDesc = singleRangePartitionDescs.get(partitionIndex);

                Long partitionRowNum = partitionRowNums.get(singleRangePartitionDesc.getPartitionName());
                if (partitionRowNum != null) {
                    this.partitionRowNum = partitionRowNum;
                    break;
                }

                partitionIndex++;
            }
            partitionFinished = false;
        }
    }

    private static final class BooleanColumnGenerator extends ColumnGenerator<Boolean, Boolean> {

        public BooleanColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                      List<LiteralExpr> literalExprs,
                                      long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);
        }

        @Override
        protected Boolean nextDefault() {
            return random.nextBoolean();
        }

        @Override
        protected Boolean nextWithStatistics() {
            return random.nextBoolean();
        }
    }

    private static final class IntegerColumnGenerator extends PartitionColumnGenerator<Long> {

        public IntegerColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                      List<LiteralExpr> literalExprs,
                                      List<SingleRangePartitionDesc> singleRangePartitionDescs, long rows,
                                      Map<String, Long> partitionRowNums) throws Exception {
            super(columnDef, columnStatistic, literalExprs, singleRangePartitionDescs, rows, partitionRowNums);
        }

        protected void initTypeInfo() {
            if (singleRangePartitionDescs != null) {
                typeMin = 19700101L;
                typeMax = timestampToDate(System.currentTimeMillis());
                for (SingleRangePartitionDesc singleRangePartitionDesc : this.singleRangePartitionDescs) {
                    PartitionKeyDesc partitionKeyDesc = singleRangePartitionDesc.getPartitionKeyDesc();
                    long partitionLower = typeMin;
                    long partitionUpper = typeMax;
                    if (partitionKeyDesc.hasLowerValues()) {
                        Preconditions.checkState(partitionKeyDesc.getLowerValues().size() == 1);
                        String lowValueStr = partitionKeyDesc.getLowerValues().get(0).getStringValue();
                        partitionLower = Long.parseLong(lowValueStr);
                    }
                    if (partitionKeyDesc.hasUpperValues()) {
                        Preconditions.checkState(partitionKeyDesc.getUpperValues().size() == 1);
                        String upperValueStr = partitionKeyDesc.getUpperValues().get(0).getStringValue();
                        partitionUpper = Long.parseLong(upperValueStr);
                    }

                    partitionBoundaries.add(
                            Triple.of(partitionLower, partitionUpper, partitionKeyDesc.getPartitionType()));
                }

                this.typeMin = partitionBoundaries.stream()
                        .map(Triple::getLeft)
                        .mapToLong(v -> v)
                        .min()
                        .orElse(typeMin);

                this.typeMax = partitionBoundaries.stream()
                        .map(Triple::getMiddle)
                        .mapToLong(v -> v)
                        .max()
                        .orElse(typeMax);
            } else {
                if (isStatisticsValid()) {
                    switch (columnDef.getType().getPrimitiveType()) {
                        case TINYINT:
                            typeMin = (long) Math.max(-128, columnStatistic.getMinValue());
                            typeMax = (long) Math.min(127, columnStatistic.getMaxValue());
                            break;
                        case SMALLINT:
                            typeMin = (long) Math.max(-32768, columnStatistic.getMinValue());
                            typeMax = (long) Math.min(32767, columnStatistic.getMaxValue());
                            break;
                        case INT:
                            typeMin = (long) Math.max(-2147483648, columnStatistic.getMinValue());
                            typeMax = (long) Math.min(2147483647, columnStatistic.getMaxValue());
                            break;
                        case BIGINT:
                            typeMin = (long) Math.max(-9223372036854775808L, columnStatistic.getMinValue());
                            typeMax = (long) Math.min(9223372036854775807L, columnStatistic.getMaxValue());
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }
                } else {
                    switch (columnDef.getType().getPrimitiveType()) {
                        case TINYINT:
                            typeMin = -128L;
                            typeMax = 127L;
                            break;
                        case SMALLINT:
                        case INT:
                        case BIGINT:
                        case LARGEINT:
                            typeMin = -32768L;
                            typeMax = 32767L;
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }
                }
            }

            for (LiteralExpr literalExpr : this.literalExprs) {
                if (literalExpr.getLongValue() > typeMin && literalExpr.getLongValue() < typeMax) {
                    this.literals.add(literalExpr.getLongValue());
                }
            }
        }

        @Override
        protected Long nextDefault() {
            Long next = nextWithPartitionInfo();
            if (next != null) {
                return next;
            }
            return intervalRandomForLong(typeMin, typeMax);
        }

        @Override
        protected Long nextWithStatistics() {
            return nextDefault();
        }

        @SuppressWarnings("deprecation")
        private long timestampToDate(long timestamp) {
            Date date = new Date(timestamp);
            return (long) date.getYear() * 10000 + date.getMonth() * 100 + date.getDay();
        }
    }

    private static final class LargeIntColumnGenerator extends ColumnGenerator<BigInteger, BigInteger> {
        public LargeIntColumnGenerator(ColumnDef columnDef,
                                       ColumnStatistic columnStatistic,
                                       List<LiteralExpr> literalExprs, long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);

            // -2^127 + 1
            typeMin = BigInteger.valueOf(2).pow(127).multiply(BigInteger.valueOf(-1)).add(BigInteger.valueOf(1));
            // 2^127 - 1
            typeMax = BigInteger.valueOf(2).pow(127).subtract(BigInteger.valueOf(1));

            if (isStatisticsValid()) {
                BigInteger statisticsMin = BigDecimal.valueOf(columnStatistic.getMinValue()).toBigInteger();
                BigInteger statisticsMax = BigDecimal.valueOf(columnStatistic.getMaxValue()).toBigInteger();
                if (statisticsMin.compareTo(typeMin) > 0) {
                    typeMin = statisticsMin;
                }
                if (statisticsMax.compareTo(typeMax) < 0) {
                    typeMax = statisticsMax;
                }
            } else {
                typeMin = BigInteger.valueOf(-32768);
                typeMax = BigInteger.valueOf(32767);
            }

            for (LiteralExpr literalExpr : this.literalExprs) {
                BigInteger value = BigDecimal.valueOf(literalExpr.getDoubleValue()).toBigInteger();
                if (value.compareTo(typeMin) >= 0 && value.compareTo((typeMax)) <= 0) {
                    this.literals.add(value);
                }
            }
        }

        @Override
        protected BigInteger nextDefault() {
            return BigInteger.valueOf((long) intervalRandom(typeMin, typeMax));
        }

        @Override
        protected BigInteger nextWithStatistics() {
            return nextDefault();
        }
    }

    private static final class FloatColumnGenerator extends ColumnGenerator<Double, Double> {
        public FloatColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                    List<LiteralExpr> literalExprs,
                                    long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);

            for (LiteralExpr literalExpr : this.literalExprs) {
                this.literals.add(literalExpr.getDoubleValue());
            }

            if (isStatisticsValid()) {
                typeMin = columnStatistic.getMinValue();
                typeMax = columnStatistic.getMaxValue();
            } else {
                typeMin = (double) Short.MIN_VALUE;
                typeMax = (double) Short.MAX_VALUE;
            }
        }

        @Override
        protected Double nextDefault() {
            return intervalRandom(typeMin, typeMax);
        }

        @Override
        protected Double nextWithStatistics() {
            return nextDefault();
        }
    }

    private static final class DecimalColumnGenerator extends ColumnGenerator<BigDecimal, BigDecimal> {

        private final int precision;
        private final int scale;

        public DecimalColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                      List<LiteralExpr> literalExprs,
                                      long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);

            precision = ((ScalarType) columnDef.getType()).decimalPrecision();
            scale = ((ScalarType) columnDef.getType()).decimalScale();

            for (LiteralExpr literalExpr : this.literalExprs) {
                BigDecimal value = BigDecimal.valueOf(literalExpr.getDoubleValue());
                // For decimal32(1), it can accept 1.2345 but not 123
                if (value.precision() - value.scale() <= precision) {
                    literals.add(value.setScale(scale, RoundingMode.HALF_UP));
                }
            }

            if (isStatisticsValid()) {
                typeMin = BigDecimal.valueOf(columnStatistic.getMinValue());
                typeMax = BigDecimal.valueOf(columnStatistic.getMaxValue());
            } else {
                typeMin = BigDecimal.valueOf(Short.MIN_VALUE);
                typeMax = BigDecimal.valueOf(Short.MAX_VALUE);
            }
        }

        @Override
        protected BigDecimal nextDefault() {
            BigDecimal next = BigDecimal.valueOf(intervalRandom(typeMin, typeMax));
            while (next.precision() - next.scale() > precision) {
                next = next.movePointLeft(1);
            }
            return next.setScale(scale, RoundingMode.HALF_UP);
        }

        @Override
        protected BigDecimal nextWithStatistics() {
            return nextDefault();
        }
    }

    private static final class StringColumnGenerator extends ColumnGenerator<String, String> {

        private final int maxSize;

        public StringColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                     List<LiteralExpr> literalExprs,
                                     long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);

            maxSize = ((ScalarType) columnDef.getType()).getLength();

            for (LiteralExpr literalExpr : this.literalExprs) {
                String str = literalExpr.getStringValue();
                str = str.replaceAll("([^\\\\]{2})%", "$1");
                str = str.replaceAll("^(.?)%", "$1");
                if (str.length() <= maxSize) {
                    this.literals.add(str);
                }
            }
        }

        private String getRandomString(int size) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < size; i++) {
                sb.append((char) (97 + random.nextInt(26)));
            }
            return sb.toString();
        }

        @Override
        protected String nextDefault() {
            return getRandomString(random.nextInt(maxSize) + 1);
        }

        @Override
        protected String nextWithStatistics() {
            return getRandomString(random.nextInt(maxSize) + 1);
        }
    }

    private static final class TimeColumnGenerator extends PartitionColumnGenerator<String> {

        private DateFormat dateFormat;

        public TimeColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic, List<LiteralExpr> literalExprs,
                                   List<SingleRangePartitionDesc> singleRangePartitionDescs, long rows,
                                   Map<String, Long> partitionRowNums) throws Exception {
            super(columnDef, columnStatistic, literalExprs, singleRangePartitionDescs, rows, partitionRowNums);
        }

        protected void initTypeInfo() throws Exception {
            if (this.columnDef.getType().isDate()) {
                dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            } else {
                dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
            typeMax = new Date().getTime();
            if (this.columnDef.getType().isDate()) {
                typeMin = dateFormat.parse("1970-01-01").getTime();
            } else {
                typeMin = dateFormat.parse("1970-01-01 00:00:00").getTime();
            }

            if (this.singleRangePartitionDescs != null) {
                for (SingleRangePartitionDesc singleRangePartitionDesc : this.singleRangePartitionDescs) {
                    PartitionKeyDesc partitionKeyDesc = singleRangePartitionDesc.getPartitionKeyDesc();
                    long partitionLower = typeMin;
                    long partitionUpper = typeMax;
                    if (partitionKeyDesc.hasLowerValues()) {
                        Preconditions.checkState(partitionKeyDesc.getLowerValues().size() == 1);
                        String lowValueStr = partitionKeyDesc.getLowerValues().get(0).getStringValue();
                        partitionLower = dateFormat.parse(lowValueStr).getTime();
                    }
                    if (partitionKeyDesc.hasUpperValues()) {
                        Preconditions.checkState(partitionKeyDesc.getUpperValues().size() == 1);
                        String upperValueStr = partitionKeyDesc.getUpperValues().get(0).getStringValue();
                        partitionUpper = dateFormat.parse(upperValueStr).getTime();
                    }

                    partitionBoundaries.add(
                            Triple.of(partitionLower, partitionUpper, partitionKeyDesc.getPartitionType()));
                }
            }

            this.typeMin = partitionBoundaries.stream()
                    .map(Triple::getLeft)
                    .mapToLong(v -> v)
                    .min()
                    .orElse(typeMin);

            this.typeMax = partitionBoundaries.stream()
                    .map(Triple::getMiddle)
                    .mapToLong(v -> v)
                    .max()
                    .orElse(typeMax);

            for (LiteralExpr literalExpr : this.literalExprs) {
                DateLiteral dateLiteral = (DateLiteral) literalExpr;
                long timestamp = dateLiteral.toLocalDateTime()
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
                if (timestamp > typeMin && timestamp < typeMax) {
                    this.literals.add(format(timestamp));
                }
            }
        }

        private String format(long timestamp) {
            return dateFormat.format(new Date(timestamp));
        }

        @Override
        protected String nextDefault() {
            Long next = nextWithPartitionInfo();
            if (next != null) {
                return format(next);
            }
            return format(intervalRandomForLong(typeMin, typeMax));
        }

        @Override
        protected String nextWithStatistics() {
            return nextDefault();
        }

    }

    private static final class ArrayColumnGenerator extends ColumnGenerator<String, String> {
        public ArrayColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                    List<LiteralExpr> literalExprs, long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);
        }

        @Override
        protected String nextDefault() {
            return "[]";
        }

        @Override
        protected String nextWithStatistics() {
            return "[]";
        }
    }

    private static final class JsonColumnGenerator extends ColumnGenerator<String, String> {
        public JsonColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic,
                                   List<LiteralExpr> literalExprs, long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);
        }

        @Override
        protected String nextDefault() {
            return "{}";
        }

        @Override
        protected String nextWithStatistics() {
            return "{}";
        }
    }

    private static final class NullColumnGenerator extends ColumnGenerator<Object, Object> {
        public NullColumnGenerator(ColumnDef columnDef, ColumnStatistic columnStatistic, List<LiteralExpr> literalExprs,
                                   long rows) {
            super(columnDef, columnStatistic, literalExprs, rows);
        }

        @Override
        protected Object nextDefault() {
            return null;
        }

        @Override
        protected Object nextWithStatistics() {
            return null;
        }
    }
}
