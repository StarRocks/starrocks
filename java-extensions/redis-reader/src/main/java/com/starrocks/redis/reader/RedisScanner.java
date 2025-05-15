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

package com.starrocks.redis.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class RedisScanner {

    public RedisScanContext scanContext;
    private List<String> resultColumnClassNames;
    private List<Object[]> resultChunk;
    private int resultNumRows = 0;
    private Jedis jedis;
    private ScanParams scanParams;
    private ScanResult<String> scanResult;
    private String cursor = "0";
    private boolean initialScan = false;
    private List<String> keys;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public RedisScanner(RedisScanContext scanContext) {
        this.scanContext = scanContext;
    }

    public void open() throws ClassNotFoundException {
        String[] urlParts = scanContext.jdbcURL.split(":");
        String host = urlParts[0];
        int port = Integer.parseInt(urlParts[1]);

        jedis = new Jedis(host, port);
        jedis.auth(scanContext.password);

        int columnSize = scanContext.columnsNames.size();
        resultColumnClassNames = new ArrayList<>(columnSize);
        resultChunk = new ArrayList<>(columnSize);
        for (int i = 0; i < columnSize; i++) {
            resultColumnClassNames.add(scanContext.columnsTypes.get(i));
            if (scanContext.columnsTypes.get(i).equals("java.lang.Long")) {
                resultChunk.add((Object[]) Array.newInstance(Long.class, 4096));
            } else  {
                resultChunk.add((Object[]) Array.newInstance(String.class, 4096));
            }
        }
    }

    public List<String> getResultColumnClassNames() {
        return resultColumnClassNames;
    }

    public boolean hasNext() throws Exception {
        return "0".equals(cursor) && !initialScan;
    }

    // return columnar chunk
    public List<Object[]> getNextChunk() throws Exception {
        resultNumRows = 0;
        String pattern = String.format("%s:%s:*", scanContext.dbName, scanContext.tblName);
        scanParams = new ScanParams().match(pattern).count(1000);
        do {
            scanResult = jedis.scan(cursor, scanParams);
            keys = scanResult.getResult();
            initialScan = true;
            cursor = scanResult.getCursor();
            if (keys.isEmpty()) {
                return resultChunk;
            }

            List<String> stringValues = jedis.mget(keys.toArray(new String[0]));
            List<List<String>>  resultList = formatResult(stringValues);

            for (List<String> value : resultList) {
                for (int i = 0; i < scanContext.columnsNames.size(); i++) {
                    Object[] dataColumn = resultChunk.get(i);
                    if (value.get(i) == null) {
                        dataColumn[resultNumRows] = null;
                    } else if (dataColumn instanceof Long[]) {
                        dataColumn[resultNumRows] = Long.valueOf(value.get(i));
                    } else {
                        dataColumn[resultNumRows] = value.get(i);
                    }
                }
                resultNumRows++;
            }
        } while (!"0".equals(cursor));

        return resultChunk;
    }

    public int getResultNumRows() {
        return resultNumRows;
    }

    public List<List<String>> formatResult(List<String> stringValues) throws Exception {
        List<List<String>> result = new ArrayList<>();

        if (keys.size() < stringValues.size()) {
            throw new IllegalArgumentException("Keys list size is smaller than stringValues list size.");
        }

        for (int i = 0; i < stringValues.size(); i++) {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(stringValues.get(i));
            List<String> values = new ArrayList<>();
            values.add(keys.get(i));

            for (JsonNode value : jsonNode) {
                values.add(value.asText());
            }
            result.add(values);
        }
        return result;
    }

    public void close() throws Exception {
        jedis.close();
    }
}
