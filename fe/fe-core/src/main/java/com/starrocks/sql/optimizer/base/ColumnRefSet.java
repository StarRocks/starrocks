// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

// BitSet used to accelerate column processing
public class ColumnRefSet implements Cloneable {
    public BitSet bitSet;

    public ColumnRefSet() {
        bitSet = new BitSet();
    }

    public ColumnRefSet(int id) {
        bitSet = new BitSet();
        bitSet.set(id);
    }

    public ColumnRefSet(Collection<ColumnRefOperator> refs) {
        bitSet = new BitSet();
        for (ColumnRefOperator ref : refs) {
            bitSet.set(ref.getId());
        }
    }

    public int[] getColumnIds() {
        return bitSet.stream().toArray();
    }

    public IntStream getStream() {
        return bitSet.stream();
    }

    public int getFirstId() {
        return bitSet.stream().findFirst().getAsInt();
    }

    @Override
    public ColumnRefSet clone() {
        try {
            ColumnRefSet result = (ColumnRefSet) super.clone();
            result.bitSet = (BitSet) bitSet.clone();
            return result;
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnRefSet)) {
            return false;
        }
        ColumnRefSet rhs = (ColumnRefSet) obj;
        return bitSet.equals(rhs.bitSet);
    }

    // The meaning is same with SQL Union Operation
    public void union(int id) {
        bitSet.set(id);
    }

    public void union(ColumnRefOperator ref) {
        bitSet.set(ref.getId());
    }

    public void union(Collection<ColumnRefOperator> refs) {
        union(new ColumnRefSet(refs));
    }

    public void union(ColumnRefSet set) {
        bitSet.or(set.bitSet);
    }

    // The meaning is same with SQL Except Operation
    public void except(List<ColumnRefOperator> refs) {
        except(new ColumnRefSet(refs));
    }

    public void except(ColumnRefSet set) {
        bitSet.andNot(set.bitSet);
    }

    // The meaning is same with SQL Intersect Operation
    public void intersect(List<ColumnRefOperator> refs) {
        intersect(new ColumnRefSet(refs));
    }

    public void intersect(ColumnRefOperator column) {
        intersect(new ColumnRefSet(column.getId()));
    }

    public void intersect(int id) {
        intersect(new ColumnRefSet(id));
    }

    public void intersect(ColumnRefSet set) {
        bitSet.and(set.bitSet);
    }

    public boolean isIntersect(ColumnRefSet other) {
        return bitSet.intersects(other.bitSet);
    }

    public int cardinality() {
        return bitSet.cardinality();
    }

    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    public void and(ColumnRefSet set) {
        bitSet.and(set.bitSet);
    }

    public boolean isSame(ColumnRefSet columnRefSet) {
        final ColumnRefSet tmp = new ColumnRefSet();
        tmp.union(columnRefSet);
        tmp.bitSet.xor(bitSet);
        return tmp.cardinality() == 0;
    }

    public void clear() {
        bitSet.clear();
    }

    public boolean contains(ColumnRefOperator ref) {
        return bitSet.get(ref.getId());
    }

    public boolean contains(int id) {
        return bitSet.get(id);
    }

    public boolean containsAll(ColumnRefSet rhs) {
        return rhs.bitSet.stream().allMatch(bit -> bitSet.get(bit));
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }
}
