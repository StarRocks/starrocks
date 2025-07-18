package com.starrocks.udf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.function.BinaryOperator;

public class SumMap<K, T> {
    BinaryOperator<T> sumFunction;

    public SumMap(BinaryOperator<T> sumFunction) {
        this.sumFunction = sumFunction;
    }

    public static class State<K, T> {
        Map<K, T> values = new java.util.HashMap<>();

        public byte[] serialize() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            for (Map.Entry<K, T> entry : values.entrySet()) {
                oos.writeObject(entry.getKey());
                oos.writeObject(entry.getValue());
            }
            oos.flush();
            oos.close();
            return baos.toByteArray();
        }

        public int serializeLength() throws IOException {
            byte[] serializedData = serialize();
            return serializedData.length;
        }

        public void serialize(java.nio.ByteBuffer buff) throws IOException {
            byte[] serializedData = serialize();
            buff.put(serializedData);
        }

        public void deserialize(java.nio.ByteBuffer buff) throws IOException, ClassNotFoundException {
            int length = buff.remaining();
            byte[] data = new byte[length];
            buff.get(data);
            try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(data))) {
                while (true) {
                    try {
                        K key = (K) ois.readObject();
                        T value = (T) ois.readObject();
                        values.put(key, value);
                    } catch (java.io.EOFException eof) {
                        break;
                    }
                }
            }
        }
    }

    public void destroy(State state) {
    }

    public final void update(State state, Map<K, T> val) {
        if (val != null) {
            for (Map.Entry<K, T> entry : val.entrySet()) {
                K key = entry.getKey();
                T value = entry.getValue();
                state.values.merge(key, value, sumFunction);
            }
        }
    }

    public void serialize(State state, java.nio.ByteBuffer buff) throws IOException {
        state.serialize(buff);
    }

    public void merge(State state, java.nio.ByteBuffer buffer) throws IOException, ClassNotFoundException {
        State<K, T> oldState = new State();
        oldState.deserialize(buffer);
        for (Map.Entry<K, T> entry : oldState.values.entrySet()) {
            K key = entry.getKey();
            T value = entry.getValue();
            state.values.merge(key, value, sumFunction);
        }
    }

    public Map<Object, T> finalize(State state) {
        return state.values;
    }
}