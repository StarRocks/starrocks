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


package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableMap;
<<<<<<< HEAD
=======
import com.starrocks.catalog.ColumnId;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class MockDictManager implements IDictManager {

    private static final ImmutableMap<ByteBuffer, Integer> MOCK_DICT =
            ImmutableMap.of(ByteBuffer.wrap("mock".getBytes(StandardCharsets.UTF_8)), 1);
    private static final ColumnDict COLUMN_DICT = new ColumnDict(MOCK_DICT, 1);

    private MockDictManager() {
    }

    private static final MockDictManager INSTANCE = new MockDictManager();

    protected static MockDictManager getInstance() {
        return INSTANCE;
    }

    @Override
<<<<<<< HEAD
    public boolean hasGlobalDict(long tableId, String columnName, long versionTime) {
=======
    public boolean hasGlobalDict(long tableId, ColumnId columnName, long versionTime) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return true;
    }

    @Override
<<<<<<< HEAD
    public void updateGlobalDict(long tableId, String columnName,  long collectedVersion, long versionTime) {
    }

    @Override
    public boolean hasGlobalDict(long tableId, String columnName) {
=======
    public void updateGlobalDict(long tableId, ColumnId columnName, long collectedVersion, long versionTime) {
    }

    @Override
    public boolean hasGlobalDict(long tableId, ColumnId columnName) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return true;
    }

    @Override
<<<<<<< HEAD
    public void removeGlobalDict(long tableId, String columnName) {
=======
    public void removeGlobalDict(long tableId, ColumnId columnName) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public void disableGlobalDict(long tableId) {
    }

    @Override
    public void enableGlobalDict(long tableId) {

    }

    @Override
<<<<<<< HEAD
    public Optional<ColumnDict> getGlobalDict(long tableId, String columnName) {
=======
    public Optional<ColumnDict> getGlobalDict(long tableId, ColumnId columnName) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return Optional.of(COLUMN_DICT);
    }
}
