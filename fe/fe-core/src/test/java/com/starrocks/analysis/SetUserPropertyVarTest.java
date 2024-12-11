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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetUserPropertyVar;
import org.junit.Assert;
import org.junit.Test;

public class SetUserPropertyVarTest {
    @Test
<<<<<<< HEAD
    public void testNormal() throws UserException {
=======
    public void testNormal() throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        SetUserPropertyVar var = new SetUserPropertyVar("quota.normal", "1000");
        Assert.assertEquals("quota.normal", var.getPropertyKey());
        Assert.assertEquals("1000", var.getPropertyValue());
    }

    @Test(expected = SemanticException.class)
<<<<<<< HEAD
    public void testUnknownProperty() throws UserException{
=======
    public void testUnknownProperty() throws StarRocksException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        SetUserPropertyVar var = new SetUserPropertyVar("unknown_property", "1000");
        SetStmtAnalyzer.analyze(new SetStmt(Lists.newArrayList(var)), null);
        Assert.fail("No exception throws.");
    }
}