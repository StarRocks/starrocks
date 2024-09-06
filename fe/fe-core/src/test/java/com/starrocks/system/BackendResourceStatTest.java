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

package com.starrocks.system;

import autovalue.shaded.com.google.common.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BackendResourceStatTest {
    @After
    public void after() {
        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testSetAndGetCores() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        assertThat(stat.getCachedAvgNumHardwareCores()).isEqualTo(-1);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumHardwareCoresOfBe(0L, 8);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(8);

        stat.setNumHardwareCoresOfBe(1L, 4);
        assertThat(stat.getCachedAvgNumHardwareCores()).isEqualTo(-1);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);

        stat.setNumHardwareCoresOfBe(0L, 16);
        assertThat(stat.getCachedAvgNumHardwareCores()).isEqualTo(-1);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(10);

        assertThat(stat.getNumHardwareCoresPerBe()).containsExactlyEntriesOf(ImmutableMap.of(0L, 16, 1L, 4));
    }

    @Test
    public void testSetAndMemLimit() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(0);

        stat.setMemLimitBytesOfBe(0L, 100);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(100);

        stat.setMemLimitBytesOfBe(1L, 50);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(150 / 2);

        stat.setMemLimitBytesOfBe(0L, 150);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(200 / 2);
    }

    @Test
    public void testGetNumBes() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumHardwareCoresOfBe(0L, 8);
        assertThat(stat.getNumBes()).isEqualTo(1);

        stat.setNumHardwareCoresOfBe(1L, 4);
        assertThat(stat.getNumBes()).isEqualTo(2);

        stat.setNumHardwareCoresOfBe(0L, 16);
        assertThat(stat.getNumBes()).isEqualTo(2);

        stat.setMemLimitBytesOfBe(0L, 100);
        assertThat(stat.getNumBes()).isEqualTo(2);

        stat.setMemLimitBytesOfBe(1L, 50);
        assertThat(stat.getNumBes()).isEqualTo(2);

        stat.setMemLimitBytesOfBe(3L, 50);
        assertThat(stat.getNumBes()).isEqualTo(3);

        // Remove non-exist BE.
        stat.removeBe(4L);
        assertThat(stat.getNumBes()).isEqualTo(3);

        // Remove exist BE.
        stat.removeBe(3L);
        assertThat(stat.getNumBes()).isEqualTo(2);
        stat.removeBe(0L);
        assertThat(stat.getNumBes()).isEqualTo(1);

        stat.reset();
        assertThat(stat.getNumBes()).isZero();
    }

    @Test
    public void testRemoveBe() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumHardwareCoresOfBe(0L, 8);
        stat.setNumHardwareCoresOfBe(1L, 4);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);

        stat.setMemLimitBytesOfBe(0L, 100);
        stat.setMemLimitBytesOfBe(1L, 50);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(150 / 2);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);

        // Remove non-exist BE.
        stat.removeBe(3L);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(150 / 2);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);

        // Remove exist BE.
        stat.removeBe(0L);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(50);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(4);
    }

    @Test
    public void testReset() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumHardwareCoresOfBe(0L, 8);
        stat.setNumHardwareCoresOfBe(1L, 4);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(4);

        stat.setMemLimitBytesOfBe(0L, 100);
        stat.setMemLimitBytesOfBe(1L, 50);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(150 / 2);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);

        stat.reset();

        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(1);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);
        assertThat(stat.getAvgMemLimitBytes()).isEqualTo(0);
        assertThat(stat.getNumBes()).isEqualTo(0);
    }

    @Test
    public void testGetDefaultDop() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumHardwareCoresOfBe(0L, 8);
        stat.setNumHardwareCoresOfBe(1L, 4);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(6);
        assertThat(stat.getDefaultDOP()).isEqualTo(6 / 2);
    }

    @Test
    public void getSinkDefaultDOP() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumHardwareCoresOfBe(0L, 23);
        stat.setNumHardwareCoresOfBe(1L, 23);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(23);
        assertThat(stat.getSinkDefaultDOP()).isEqualTo(23 / 3);

        stat.reset();

        stat.setNumHardwareCoresOfBe(0L, 32);
        stat.setNumHardwareCoresOfBe(1L, 32);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(32);
        assertThat(stat.getSinkDefaultDOP()).isEqualTo(32 / 4);

        stat.reset();

        stat.setNumHardwareCoresOfBe(0L, 32 * 2 * 4);
        stat.setNumHardwareCoresOfBe(1L, 32 * 2 * 4);
        assertThat(stat.getAvgNumHardwareCoresOfBe()).isEqualTo(32 * 2 * 4);
        assertThat(stat.getSinkDefaultDOP()).isEqualTo(32);
    }

    @Test
    public void testGetMinNumHardwareCoresOfBe() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumHardwareCoresOfBe(0L, 0);
        stat.setNumHardwareCoresOfBe(1L, 0);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumHardwareCoresOfBe(0L, 4);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumHardwareCoresOfBe(1L, 8);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(4);

        stat.removeBe(0L);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(8);

        stat.removeBe(1L);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);
    }

}
