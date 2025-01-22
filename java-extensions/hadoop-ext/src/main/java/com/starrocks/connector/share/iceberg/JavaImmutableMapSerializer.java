package com.starrocks.connector.share.iceberg;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class JavaImmutableMapSerializer extends Serializer<Map<?, ?>> {

  @Override
  public void write(Kryo kryo, Output output, Map<?, ?> map) {
    ImmSerMapEntry[] entries = map
      .entrySet()
      .stream()
      .map(ImmSerMapEntry::new)
      .toArray(ImmSerMapEntry[]::new);

    kryo.writeObject(output, new ImmSerMap(entries));
  }

  @Override
  public Map<?, ?> read(Kryo kryo, Input input, Class<Map<?, ?>> type) {
    return Map.ofEntries(kryo.readObject(input, ImmSerMap.class).getEntries());
  }


  private static class ImmSerMap implements Serializable {

    private final ImmSerMapEntry<?, ?>[] array;

    private ImmSerMap(ImmSerMapEntry[] array) {
      this.array = array;
    }

    private Map.Entry[] getEntries() {
      return array;
    }
  }

  private static class ImmSerMapEntry<K, V> implements Map.Entry<K, V>, Serializable {

    private final Object[] entry;

    private ImmSerMapEntry(Map.Entry<K, V> entry) {
      this.entry = new Object[] { entry.getKey(), entry.getValue() };
    }

    @Override
    public K getKey() {
      return (K) entry[0];
    }

    @Override
    public V getValue() {
      return (V) entry[1];
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }
}