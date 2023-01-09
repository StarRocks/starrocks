package com.starrocks.jni.connector;

public interface ColumnValue {
    public boolean getBoolean();

    public short getShort();

    public int getInt();

    public float getFloat();

    public long getLong();

    public double getDouble();

    public String getString();
}
