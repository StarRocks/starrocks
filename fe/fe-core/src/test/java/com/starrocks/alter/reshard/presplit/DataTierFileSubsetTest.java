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

package com.starrocks.alter.reshard.presplit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.UniformReservoir;
import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.brokerFileStatus;

class DataTierFileSubsetTest {

    private static final long GIB = 1L << 30;

    private long savedByteLimit;
    private int savedMinFiles;
    private int savedMaxFiles;
    private boolean savedHasInit;
    private Histogram savedHistogram;

    @BeforeEach
    void setUp() {
        savedByteLimit = Config.tablet_pre_split_data_tier_scan_byte_limit;
        savedMinFiles = Config.tablet_pre_split_data_tier_min_scan_files;
        savedMaxFiles = Config.tablet_pre_split_data_tier_max_scan_files;
        savedHasInit = MetricRepo.hasInit;
        savedHistogram = MetricRepo.HISTO_TABLET_PRE_SPLIT_DATA_TIER_SCANNED_BYTES_PERCENT;
        Config.tablet_pre_split_data_tier_scan_byte_limit = 3 * GIB;
        Config.tablet_pre_split_data_tier_min_scan_files = 1;
    }

    @AfterEach
    void tearDown() {
        Config.tablet_pre_split_data_tier_scan_byte_limit = savedByteLimit;
        Config.tablet_pre_split_data_tier_min_scan_files = savedMinFiles;
        Config.tablet_pre_split_data_tier_max_scan_files = savedMaxFiles;
        MetricRepo.hasInit = savedHasInit;
        MetricRepo.HISTO_TABLET_PRE_SPLIT_DATA_TIER_SCANNED_BYTES_PERCENT = savedHistogram;
    }

    @Test
    void inputOverTheLimitScansASubsetAndKeepsTheTotal() throws Exception {
        DataTierFileSubset subset = DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false);

        Assertions.assertEquals(DataTierFileSubset.Mode.SUBSET, subset.mode());
        Assertions.assertTrue(subset.isSubset());
        Assertions.assertEquals(List.of("s3://b/d/f2.parquet", "s3://b/d/f5.parquet", "s3://b/d/f7.parquet"),
                subset.paths());
        Assertions.assertEquals(3 * GIB, subset.scannedBytes());
        Assertions.assertEquals(10 * GIB, subset.totalBytes());
        Assertions.assertTrue(subset.partitionSourceBytes().isEmpty(), "no path partition, no breakdown");
    }

    @Test
    void inputWithinTheLimitScansEveryFileAndNeverParsesPaths() throws Exception {
        Config.tablet_pre_split_data_tier_scan_byte_limit = 10 * GIB;
        // These paths carry no dt= segment; parsing them would throw.
        DataTierFileSubset subset = DataTierFileSubset.choose(tenFiles("s3://b/d/f"), dtFromPath(), false, false);

        Assertions.assertEquals(DataTierFileSubset.Mode.UNDER_LIMIT, subset.mode());
        Assertions.assertEquals(10, subset.paths().size());
        Assertions.assertEquals(subset.totalBytes(), subset.scannedBytes());
    }

    @Test
    void configIsReadOnEveryCall() throws Exception {
        Assertions.assertTrue(DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false).isSubset());

        Config.tablet_pre_split_data_tier_scan_byte_limit = 0L;
        DataTierFileSubset subset = DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false);

        Assertions.assertEquals(DataTierFileSubset.Mode.DISABLED, subset.mode());
        Assertions.assertEquals(10, subset.paths().size());
    }

    @Test
    void theFileCapIsReadOnEveryCall() throws Exception {
        Config.tablet_pre_split_data_tier_scan_byte_limit = 5 * GIB;
        Assertions.assertEquals(5, DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false).paths().size(),
                "the default cap of 512 does not bind five 1 GiB files");

        Config.tablet_pre_split_data_tier_max_scan_files = 2;
        DataTierFileSubset subset = DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false);

        Assertions.assertEquals(DataTierFileSubset.Mode.SUBSET, subset.mode());
        Assertions.assertEquals(List.of("s3://b/d/f2.parquet", "s3://b/d/f5.parquet"), subset.paths());
    }

    @Test
    void aSelectionCoveringEveryFileScansEveryFile() throws Exception {
        Config.tablet_pre_split_data_tier_min_scan_files = 16;

        DataTierFileSubset subset = DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false);

        Assertions.assertEquals(DataTierFileSubset.Mode.ALL_SELECTED, subset.mode());
        Assertions.assertFalse(subset.isSubset());
        Assertions.assertEquals(10, subset.paths().size());
    }

    @Test
    void zeroByteFilesAreLeftOutOfASubsetButKeptWhenEveryFileIsScanned() throws Exception {
        List<TBrokerFileStatus> files = new ArrayList<>(tenFiles("s3://b/d/f"));
        files.add(brokerFileStatus("s3://b/d/empty.parquet", 0L));

        DataTierFileSubset subset = DataTierFileSubset.choose(files, null, false, false);
        Assertions.assertTrue(subset.isSubset());
        Assertions.assertFalse(subset.paths().contains("s3://b/d/empty.parquet"));

        Config.tablet_pre_split_data_tier_min_scan_files = 16;
        DataTierFileSubset everyFile = DataTierFileSubset.choose(files, null, false, false);
        Assertions.assertEquals(DataTierFileSubset.Mode.ALL_SELECTED, everyFile.mode());
        Assertions.assertTrue(everyFile.paths().contains("s3://b/d/empty.parquet"),
                "scanning every file keeps today's exact path list");
    }

    @Test
    void directoriesAreNotFiles() throws Exception {
        List<TBrokerFileStatus> files = new ArrayList<>(tenFiles("s3://b/d/f"));
        files.add(new TBrokerFileStatus("s3://b/d/sub", /*isDir=*/ true, 0L, false));
        Config.tablet_pre_split_data_tier_scan_byte_limit = 0L;

        Assertions.assertEquals(10, DataTierFileSubset.choose(files, null, false, false).paths().size());
    }

    @Test
    void pathPartitionedInputIsStratifiedAndSizedExactly() throws Exception {
        Config.tablet_pre_split_data_tier_scan_byte_limit = 5 * GIB;
        List<TBrokerFileStatus> files = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            files.add(brokerFileStatus("s3://b/dt=2026-09-11/f" + i + ".parquet", GIB));
        }
        for (int i = 0; i < 2; i++) {
            files.add(brokerFileStatus("s3://b/dt=2026-09-10/f" + i + ".parquet", GIB));
        }

        DataTierFileSubset subset = DataTierFileSubset.choose(files, dtFromPath(), false, false);

        Assertions.assertEquals(List.of(
                "s3://b/dt=2026-09-11/f1.parquet", "s3://b/dt=2026-09-11/f2.parquet",
                "s3://b/dt=2026-09-11/f4.parquet", "s3://b/dt=2026-09-11/f6.parquet",
                "s3://b/dt=2026-09-10/f1.parquet"), subset.paths());
        List<Estimates.PartitionSourceBytes> breakdown = subset.partitionSourceBytes();
        Assertions.assertEquals(2, breakdown.size());
        Assertions.assertEquals("2026-09-11", breakdown.get(0).values().get(0).getStringValue());
        Assertions.assertEquals(8 * GIB, breakdown.get(0).bytes());
        Assertions.assertEquals(2 * GIB, breakdown.get(1).bytes());
    }

    @Test
    void unconvertiblePathValueIsLeftOutOfTheBreakdown() throws Exception {
        Config.tablet_pre_split_data_tier_scan_byte_limit = 5 * GIB;
        List<TBrokerFileStatus> files = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            files.add(brokerFileStatus("s3://b/dt=2026-09-11/f" + i + ".parquet", GIB));
        }
        for (int i = 0; i < 2; i++) {
            files.add(brokerFileStatus("s3://b/dt=/f" + i + ".parquet", GIB));
        }

        DataTierFileSubset subset = DataTierFileSubset.choose(files, dtFromPath(), false, false);

        Assertions.assertTrue(subset.isSubset());
        Assertions.assertEquals(1, subset.partitionSourceBytes().size(), "the empty date gets no exact size");
        Assertions.assertEquals(8 * GIB, subset.partitionSourceBytes().get(0).bytes());
    }

    @Test
    void pathMissingThePartitionKeyFailsTheSample() {
        List<TBrokerFileStatus> files = tenFiles("s3://b/d/f");

        StarRocksException failure = Assertions.assertThrows(StarRocksException.class,
                () -> DataTierFileSubset.choose(files, dtFromPath(), false, false));
        Assertions.assertFalse(failure.getMessage().contains("s3://b/d/f"), "the path stays out of the message");
    }

    @Test
    void aPartitionColumnReadFromTheFileDataScansEveryFile() throws Exception {
        DataTierFileSubset overLimit = DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, true, false);

        Assertions.assertEquals(DataTierFileSubset.Mode.PARTITION_FROM_FILE_DATA, overLimit.mode());
        Assertions.assertEquals(10, overLimit.paths().size());
        Assertions.assertEquals(overLimit.totalBytes(), overLimit.scannedBytes());

        Config.tablet_pre_split_data_tier_scan_byte_limit = 20 * GIB;
        Assertions.assertEquals(DataTierFileSubset.Mode.UNDER_LIMIT,
                DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, true, false).mode());
    }

    @Test
    void aSelectedPathFilesCannotReadExactlyScansEveryFile() throws Exception {
        for (String name : List.of("f5,x", "f5*", "f5?", "f5[x]", "f5{x}", "f5\\x", "f5:x")) {
            List<TBrokerFileStatus> files = tenFiles("s3://b/d/f");
            // f5 holds the first grid point, so it is always selected.
            files.set(5, brokerFileStatus("s3://b/d/" + name + ".parquet", GIB));

            DataTierFileSubset subset = DataTierFileSubset.choose(files, null, false, /*requireExactFilesPaths=*/ true);

            Assertions.assertEquals(DataTierFileSubset.Mode.PATH_NOT_EXPRESSIBLE, subset.mode(), name);
            Assertions.assertEquals(subset.totalBytes(), subset.scannedBytes(), name);
            Assertions.assertEquals(10, subset.paths().size(), name);
        }
    }

    @Test
    void exactFilesPaths() {
        for (String path : List.of("s3://b/dt=2026-09-10/part-0.orc", "hdfs://nn:8020/a/b.orc",
                "file:/tmp/a.orc", "oss://b/a%20b#c=d.orc", "s3://bucket")) {
            Assertions.assertTrue(DataTierFileSubset.isExactFilesPath(path), path);
        }
        for (String path : List.of("s3://b/a,b.orc", "s3://b/a*.orc", "s3://b/a?.orc", "s3://b/a[1].orc",
                "s3://b/a{1}.orc", "s3://b/a\\1.orc", "s3://b/a:1.orc", " s3://b/a.orc", "s3://b/a.orc ",
                "s3://b/a.orc\u2003", "\u2003s3://b/a.orc", "/tmp/a:b.orc", "")) {
            Assertions.assertFalse(DataTierFileSubset.isExactFilesPath(path), path);
        }
    }

    @Test
    void reportRecordsTheMetricAndTheHistogram() throws Exception {
        MetricRepo.hasInit = true;
        Histogram histogram = new Histogram(new UniformReservoir());
        MetricRepo.HISTO_TABLET_PRE_SPLIT_DATA_TIER_SCANNED_BYTES_PERCENT = histogram;
        long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_DATA_TIER_FILE_SELECTION.getMetric("subset").getValue();

        DataTierFileSubset.choose(tenFiles("s3://b/d/f"), null, false, false).report("Broker Load data tier ");

        Assertions.assertEquals(before + 1L,
                MetricRepo.COUNTER_TABLET_PRE_SPLIT_DATA_TIER_FILE_SELECTION.getMetric("subset").getValue());
        Assertions.assertEquals(1L, histogram.getCount());
        Assertions.assertEquals(30L, histogram.getSnapshot().getMax());
    }

    @Test
    void onlyAScanOfEveryByteRecordsOneHundredPercent() {
        MetricRepo.hasInit = true;
        Histogram histogram = new Histogram(new UniformReservoir());
        MetricRepo.HISTO_TABLET_PRE_SPLIT_DATA_TIER_SCANNED_BYTES_PERCENT = histogram;

        // 2^53 / (2^53 + 1) is 1.0 as a double; one byte is still left unscanned.
        PreSplitMetrics.recordDataTierFileSelection("subset", 1L << 53, (1L << 53) + 1L);
        Assertions.assertEquals(99L, histogram.getSnapshot().getMax());

        PreSplitMetrics.recordDataTierFileSelection("under_limit", 7L, 7L);
        Assertions.assertEquals(100L, histogram.getSnapshot().getMax());
    }

    private static List<TBrokerFileStatus> tenFiles(String prefix) {
        List<TBrokerFileStatus> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(brokerFileStatus(prefix + i + ".parquet", GIB));
        }
        return files;
    }

    private static PathPartitionValues dtFromPath() {
        Column dt = new Column("dt", DateType.DATE);
        return PathPartitionValues.of(List.of("dt"), List.of(dt), List.of("dt"));
    }
}
