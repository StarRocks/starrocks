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
import com.starrocks.catalog.ColumnId;

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
    public boolean hasGlobalDict(long tableId, ColumnId columnName, long versionTime) {
        return true;
    }

    @Override
    public void updateGlobalDict(long tableId, ColumnId columnName, long collectedVersion, long versionTime) {
    }

    @Override
    public boolean hasGlobalDict(long tableId, ColumnId columnName) {
        return true;
    }

    @Override
    public void removeGlobalDict(long tableId, ColumnId columnName) {
    }

    @Override
    public void disableGlobalDict(long tableId) {
    }

    @Override
    public void enableGlobalDict(long tableId) {

    }

    @Override
    public Optional<ColumnDict> getGlobalDict(long tableId, ColumnId columnName) {
        return Optional.of(COLUMN_DICT);
    }
}
