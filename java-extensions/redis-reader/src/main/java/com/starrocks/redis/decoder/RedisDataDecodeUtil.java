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

package com.starrocks.redis.decoder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RedisDataDecodeUtil {

    public static List<List<String>> decodeStringData(
            List<String> keys,
            List<String> stringValues,
            String valueDataFormat) throws Exception {

        if (keys.size() < stringValues.size()) {
            throw new IllegalArgumentException("Keys list size is smaller than stringValues list size.");
        }

        RowDataDecoder decoder = RowDataDecoderFactory.getDecoder(valueDataFormat);
        List<List<String>> result = new ArrayList<>(stringValues.size());

        for (int i = 0; i < stringValues.size(); i++) {
            result.add(decoder.decodeStringValues(keys.get(i), stringValues.get(i)));
        }

        return result;
    }

    public static List<List<String>> decodeHashData(
            List<String> keys,
            List<Map<String, String>> hashValues,
            String valueDataFormat) throws Exception {

        if (keys.size() < hashValues.size()) {
            throw new IllegalArgumentException("Keys list size is smaller than hashValues list size.");
        }

        RowDataDecoder decoder = RowDataDecoderFactory.getDecoder(valueDataFormat);
        List<List<String>> result = new ArrayList<>(hashValues.size());

        for (int i = 0; i < hashValues.size(); i++) {
            result.add(decoder.decodeHashValues(keys.get(i), hashValues.get(i)));
        }

        return result;
    }

    public static List<List<String>> processValues(String valueDataFormat, Jedis jedis, List<String> keys)
            throws Exception {
        List<List<String>> resultList = new ArrayList<>();
        switch (toRedisDataType(valueDataFormat)) {
            case STRING -> {
                List<String> stringValues = jedis.mget(keys.toArray(new String[0]));
                resultList = RedisDataDecodeUtil.decodeStringData(keys, stringValues, valueDataFormat);
            }
            case HASH -> {
                Pipeline pipeline = jedis.pipelined();
                keys.forEach(pipeline::hgetAll);
                @SuppressWarnings("unchecked")
                List<Map<String, String>> hashValues =
                        (List<Map<String, String>>) (List<?>) pipeline.syncAndReturnAll();
                resultList = RedisDataDecodeUtil.decodeHashData(keys, hashValues, valueDataFormat);
            }
        }
        return resultList;
    }

    public static RedisDataType toRedisDataType(String dataFormat) {
        switch (dataFormat) {
            case "hash":
                return RedisDataType.HASH;
            default:
                return RedisDataType.STRING;
        }
    }
}

