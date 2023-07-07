// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.ImmutableMap;

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
    public boolean hasGlobalDict(long tableId, String columnName, long versionTime) {
        return true;
    }

    @Override
    public void updateGlobalDict(long tableId, String columnName,  long collectedVersion, long versionTime) {
    }

    @Override
    public boolean hasGlobalDict(long tableId, String columnName) {
        return true;
    }

    @Override
    public void removeGlobalDict(long tableId, String columnName) {
    }

    @Override
    public void disableGlobalDict(long tableId) {
    }

    @Override
    public void enableGlobalDict(long tableId) {

    }

    @Override
    public Optional<ColumnDict> getGlobalDict(long tableId, String columnName) {
        return Optional.of(COLUMN_DICT);
    }
}
