// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProcServiceTest {
    private class EmptyProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            return null;
        }
    }

    // test directory:
    // - starrocks
    // | - be
    //   | - src
    //   | - deps
    // | - fe
    //   | - src
    //   | - conf
    //   | - build.sh
    // | - common
    @BeforeEach
    public void beforeTest() {
        ProcService procService = ProcService.getInstance();

        BaseProcDir starrocksDir = new BaseProcDir();
        Assertions.assertTrue(procService.register("starrocks", starrocksDir));

        BaseProcDir beDir = new BaseProcDir();
        Assertions.assertTrue(starrocksDir.register("be", beDir));
        Assertions.assertTrue(beDir.register("src", new BaseProcDir()));
        Assertions.assertTrue(beDir.register("deps", new BaseProcDir()));

        BaseProcDir feDir = new BaseProcDir();
        Assertions.assertTrue(starrocksDir.register("fe", feDir));
        Assertions.assertTrue(feDir.register("src", new BaseProcDir()));
        Assertions.assertTrue(feDir.register("conf", new BaseProcDir()));
        Assertions.assertTrue(feDir.register("build.sh", new EmptyProcNode()));

        Assertions.assertTrue(starrocksDir.register("common", new BaseProcDir()));
    }

    @AfterEach
    public void afterTest() {
        ProcService.destroy();
    }

    @Test
    public void testRegisterNormal() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assertions.assertTrue(procService.register(name, dir));
    }

    // register second time
    @Test
    public void testRegisterSecond() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assertions.assertTrue(procService.register(name, dir));
        Assertions.assertFalse(procService.register(name, dir));
    }

    // register invalid
    @Test
    public void testRegisterInvalidInput() {
        ProcService procService = ProcService.getInstance();
        String name = "test";
        BaseProcDir dir = new BaseProcDir();

        Assertions.assertFalse(procService.register(null, dir));
        Assertions.assertFalse(procService.register("", dir));
        Assertions.assertFalse(procService.register(name, null));
    }

    @Test
    public void testOpenNormal() throws AnalysisException {
        ProcService procService = ProcService.getInstance();

        // assert root
        Assertions.assertNotNull(procService.open("/"));
        Assertions.assertNotNull(procService.open("/starrocks"));
        Assertions.assertNotNull(procService.open("/starrocks/be"));
        Assertions.assertNotNull(procService.open("/starrocks/be/src"));
        Assertions.assertNotNull(procService.open("/starrocks/be/deps"));
        Assertions.assertNotNull(procService.open("/starrocks/fe"));
        Assertions.assertNotNull(procService.open("/starrocks/fe/src"));
        Assertions.assertNotNull(procService.open("/starrocks/fe/conf"));
        Assertions.assertNotNull(procService.open("/starrocks/fe/build.sh"));
        Assertions.assertNotNull(procService.open("/starrocks/common"));
    }

    @Test
    public void testOpenSapceNormal() throws AnalysisException {
        ProcService procService = ProcService.getInstance();

        // assert space
        Assertions.assertNotNull(procService.open(" \r/"));
        Assertions.assertNotNull(procService.open(" \r/ "));
        Assertions.assertNotNull(procService.open("  /starrocks \r\n"));
        Assertions.assertNotNull(procService.open("\n\r\t /starrocks/be \n\r"));

        // assert last '/'
        Assertions.assertNotNull(procService.open(" /starrocks/be/"));
        Assertions.assertNotNull(procService.open(" /starrocks/fe/  "));

        ProcNodeInterface node = procService.open("/dbs");
        Assertions.assertNotNull(node);
        Assertions.assertTrue(node instanceof DbsProcDir);
    }

    @Test
    public void testOpenFail() {
        ProcService procService = ProcService.getInstance();

        // assert no path
        int errCount = 0;
        try {
            procService.open("/abc");
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assertions.assertNull(procService.open("/starrocks/b e"));
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assertions.assertNull(procService.open("/starrocks/fe/build.sh/"));
        } catch (AnalysisException e) {
            ++errCount;
        }

        // assert no root
        try {
            Assertions.assertNull(procService.open("starrocks"));
        } catch (AnalysisException e) {
            ++errCount;
        }
        try {
            Assertions.assertNull(procService.open(" starrocks"));
        } catch (AnalysisException e) {
            ++errCount;
        }

        Assertions.assertEquals(5, errCount);
    }

}
