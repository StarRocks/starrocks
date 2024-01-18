// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// BitSet used to accelerate column processing
public class ColumnRefSet implements Cloneable {
    public RoaringBitmap bitSet;

    public ColumnRefSet() {
        bitSet = new RoaringBitmap();
    }

    public ColumnRefSet(int id) {
        bitSet = new RoaringBitmap();
        bitSet.add(id);
    }

    public ColumnRefSet(Collection<ColumnRefOperator> refs) {
        bitSet = new RoaringBitmap();
        for (ColumnRefOperator ref : refs) {
            bitSet.add(ref.getId());
        }
    }

    public static ColumnRefSet createByIds(Collection<Integer> colIds) {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        colIds.stream().forEach(columnRefSet::union);
        return columnRefSet;
    }

    public int[] getColumnIds() {
        return bitSet.toArray();
    }

    public Stream<Integer> getStream() {
        Spliterator<Integer> spliterator = Spliterators.spliteratorUnknownSize(bitSet.iterator(), Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    public int getFirstId() {
        return bitSet.first();
    }

    @Override
    public ColumnRefSet clone() {
        try {
            ColumnRefSet result = (ColumnRefSet) super.clone();
            result.bitSet = bitSet.clone();
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

    public int size() {
        return bitSet.getCardinality();
    }

    // The meaning is same with SQL Union Operation
    public void union(int id) {
        bitSet.add(id);
    }

    public void union(ColumnRefOperator ref) {
        bitSet.add(ref.getId());
    }

    public void union(Collection<ColumnRefOperator> refs) {
        union(new ColumnRefSet(refs));
    }

    public void union(ColumnRefSet set) {
        bitSet.or(set.bitSet);
    }

    // The meaning is same with SQL Except Operation
    public void except(Collection<ColumnRefOperator> refs) {
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
        for (int id : other.bitSet) {
            if (this.bitSet.contains(id)) {
                return true;
            }
        }
        return false;
    }

    public int cardinality() {
        return bitSet.getCardinality();
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
        return bitSet.contains(ref.getId());
    }

    public boolean contains(int id) {
        return bitSet.contains(id);
    }

    public boolean containsAll(ColumnRefSet rhs) {
        return this.bitSet.contains(rhs.bitSet);
    }

    public boolean containsAny(ColumnRefSet rhs) {
        return isIntersect(rhs);
<<<<<<< HEAD
=======
    }

    public boolean containsAny(Collection<ColumnRefOperator> rhs) {
        return rhs.stream().anyMatch(this::contains);
>>>>>>> 2.5.18
    }

    public boolean containsAll(List<Integer> rhs) {
        return rhs.stream().allMatch(id -> bitSet.contains(id));
    }

    public List<ColumnRefOperator> getColumnRefOperators(ColumnRefFactory columnRefFactory) {
        return getStream().map(columnRefFactory::getColumnRef).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }
}
