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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class BackendResourceStatTest {
    @AfterEach
    public void after() {
        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testSetAndGetCores() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        assertThat(stat.getCachedAvgNumCores()).isEqualTo(-1);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(1);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 8);
        stat.setNumCoresOfBe(1, 10L, 16);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(12);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(8);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(16);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 4);
        stat.setNumCoresOfBe(1, 11L, 8);
        assertThat(stat.getCachedAvgNumCores()).isEqualTo(-1);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(9);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(6);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(12);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 16);
        stat.setNumCoresOfBe(1, 10L, 32);
        assertThat(stat.getCachedAvgNumCores()).isEqualTo(-1);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(15);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(10);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(20);
    }


    @Test
    public void testSetAndMemLimit() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(0);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 0L, 100);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(100);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 1L, 50);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(150 / 2);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 0L, 150);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(200 / 2);
    }

    @Test
    public void testGetNumBes() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 8);
        stat.setNumCoresOfBe(1, 10L, 8);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(1);
        assertThat(stat.getNumBes(1)).isEqualTo(1);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 4);
        stat.setNumCoresOfBe(1, 11L, 4);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(2);
        assertThat(stat.getNumBes(1)).isEqualTo(2);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 16);
        stat.setNumCoresOfBe(1, 10L, 16);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(2);
        assertThat(stat.getNumBes(1)).isEqualTo(2);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 0L, 100);
        stat.setMemLimitBytesOfBe(1, 10L, 100);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(2);
        assertThat(stat.getNumBes(1)).isEqualTo(2);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 1L, 50);
        stat.setMemLimitBytesOfBe(1, 11L, 50);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(2);
        assertThat(stat.getNumBes(1)).isEqualTo(2);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 3L, 50);
        stat.setMemLimitBytesOfBe(1, 13L, 50);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(3);
        assertThat(stat.getNumBes(1)).isEqualTo(3);

        // Remove non-exist BE.
        stat.removeBe(DEFAULT_WAREHOUSE_ID, 4L);
        stat.removeBe(1, 14L);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(3);
        assertThat(stat.getNumBes(1)).isEqualTo(3);

        // Remove exist BE.
        stat.removeBe(DEFAULT_WAREHOUSE_ID, 3L);
        stat.removeBe(1, 13L);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(2);
        assertThat(stat.getNumBes(1)).isEqualTo(2);
        stat.removeBe(DEFAULT_WAREHOUSE_ID, 0L);
        stat.removeBe(1, 10L);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(1);
        assertThat(stat.getNumBes(1)).isEqualTo(1);

        stat.reset();
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isZero();
        assertThat(stat.getNumBes(1)).isZero();
    }

    @Test
    public void testRemoveBe() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 8);
        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 4);
        stat.setNumCoresOfBe(1, 11L, 12);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(8);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 0L, 100);
        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 1L, 50);
        stat.setMemLimitBytesOfBe(1, 11L, 200);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(150 / 2);
        assertThat(stat.getAvgMemLimitBytes(1)).isEqualTo(200);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(8);

        // Remove non-exist BE.
        stat.removeBe(DEFAULT_WAREHOUSE_ID, 3L);
        stat.removeBe(1, 0L);
        stat.removeBe(1, 13L);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(150 / 2);
        assertThat(stat.getAvgMemLimitBytes(1)).isEqualTo(200);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(8);

        // Remove exist BE.
        stat.removeBe(DEFAULT_WAREHOUSE_ID, 0L);
        stat.removeBe(1, 11L);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(50);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(4);
    }

    @Test
    public void testReset() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 8);
        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 4);
        stat.setNumCoresOfBe(1, 11L, 12);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(8);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(4);

        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 0L, 100);
        stat.setMemLimitBytesOfBe(DEFAULT_WAREHOUSE_ID, 1L, 50);
        stat.setMemLimitBytesOfBe(1, 11L, 200);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(150 / 2);
        assertThat(stat.getAvgMemLimitBytes(1)).isEqualTo(200);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(8);

        stat.reset();

        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(1);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);
        assertThat(stat.getAvgMemLimitBytes(DEFAULT_WAREHOUSE_ID)).isEqualTo(0);
        assertThat(stat.getNumBes(DEFAULT_WAREHOUSE_ID)).isEqualTo(0);
    }

    @Test
    public void testGetDefaultDop() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 8);
        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 4);
        stat.setNumCoresOfBe(1, 1L, 12);
        assertThat(stat.getAvgNumCoresOfBe()).isEqualTo(8);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(6);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(12);
        assertThat(stat.getDefaultDOP(DEFAULT_WAREHOUSE_ID)).isEqualTo(6 / 2);
        assertThat(stat.getDefaultDOP(1)).isEqualTo(12 / 2);
    }

    @Test
    public void getSinkDefaultDOP() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 23);
        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 23);
        stat.setNumCoresOfBe(1, 10L, 22);
        stat.setNumCoresOfBe(1, 11L, 22);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(23);
        assertThat(stat.getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID)).isEqualTo(23 / 3);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(22);
        assertThat(stat.getSinkDefaultDOP(1)).isEqualTo(22 / 3);

        stat.reset();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 32);
        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 32);
        stat.setNumCoresOfBe(1, 10L, 34);
        stat.setNumCoresOfBe(1, 11L, 34);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(32);
        assertThat(stat.getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID)).isEqualTo(32 / 4);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(34);
        assertThat(stat.getSinkDefaultDOP(1)).isEqualTo(34 / 4);

        stat.reset();

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 32 * 2 * 4);
        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1L, 32 * 2 * 4);
        stat.setNumCoresOfBe(1, 10L, 34 * 2 * 4);
        stat.setNumCoresOfBe(1, 11L, 34 * 2 * 4);
        assertThat(stat.getAvgNumCoresOfBe(DEFAULT_WAREHOUSE_ID)).isEqualTo(32 * 2 * 4);
        assertThat(stat.getSinkDefaultDOP(DEFAULT_WAREHOUSE_ID)).isEqualTo(32);
        assertThat(stat.getAvgNumCoresOfBe(1)).isEqualTo(34 * 2 * 4);
        assertThat(stat.getSinkDefaultDOP(1)).isEqualTo(32);
    }

    @Test
    public void testGetMinNumHardwareCoresOfBe() {
        BackendResourceStat stat = BackendResourceStat.getInstance();

        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 0);
        stat.setNumCoresOfBe(1, 1L, 0);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 0L, 4);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);

        stat.setNumCoresOfBe(1, 1L, 8);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(4);

        stat.removeBe(DEFAULT_WAREHOUSE_ID, 0L);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(8);

        stat.removeBe(1, 1L);
        assertThat(stat.getMinNumHardwareCoresOfBe()).isEqualTo(1);
    }

}
