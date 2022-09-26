// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.example.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;

public class JsonAgg {

    public static class State {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonObj = objectMapper.createObjectNode();

        public int serializeLength() {
            return 4 + jsonObj.toString().getBytes(StandardCharsets.UTF_8).length;
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public final void update(State state, String columnA, String columnB) {
        if (columnA != null) {
            state.jsonObj.put(columnA, columnB);
        }
    }

    public void serialize(State state, java.nio.ByteBuffer buff) {
        byte[] bytes = state.jsonObj.toString().getBytes(StandardCharsets.UTF_8);
        buff.putInt(bytes.length);
        buff.put(bytes);
    }

    public void merge(State state, java.nio.ByteBuffer buffer)
            throws JsonProcessingException {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);

        ObjectMapper objectMapper = new ObjectMapper();
        final String json = new String(bytes, StandardCharsets.UTF_8);
        JsonNode jsonNode = objectMapper.readTree(json);

        state.jsonObj.putAll((ObjectNode) jsonNode);
    }

    public String finalize(State state) {
        return state.jsonObj.toString();
    }
}
