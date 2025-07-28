package com.starrocks.udf;

import java.io.IOException;
import java.util.Map;

public class SumMapInt64 extends SumMap<Object, Long> {
    public SumMapInt64() {
        super(Long::sum);
    }

    public static class State extends SumMap.State<Object, Long> {
        public int serializeLength() throws IOException {
            return super.serializeLength();
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
        super.destroy(state);
    }

    // only support scalar type!!!
    public final void update(State state, Map<Object, Long> val) {
        super.update(state, val);
    }

    public void serialize(State state, java.nio.ByteBuffer buff) throws IOException {
        super.serialize(state, buff);
    }

    public void merge(State state, java.nio.ByteBuffer buffer) throws IOException, ClassNotFoundException {
        super.merge(state, buffer);
    }

    public Map<Object, Long> finalize(State state) {
        return super.finalize(state);
    }
}