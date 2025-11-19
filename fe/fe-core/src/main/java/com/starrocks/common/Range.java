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

package com.starrocks.common;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * An abstract class representing a range with lower and upper bounds and supporting gson serialization.
 * Bounds can be inclusive or exclusive, and null bounds represent infinity.
 *
 * <p>Note: The {@link #compareTo(Range)} method is used for ordering ranges
 * and may return 0 for overlapping ranges that are not equal. This is intentional
 * for range sorting purposes. Do not rely on {@code compareTo(x) == 0} to test equality;
 * use {@link #equals(Object)} instead.
 *
 * @param <T> the type of the range bounds, must be Comparable
 */
public abstract class Range<T extends Comparable<T>> implements Comparable<Range<T>> {

    /**
     * Factory method to create a Range instance based on bounds and inclusiveness.
     *
     * @param lowerBound the lower bound, or null for negative infinity
     * @param upperBound the upper bound, or null for positive infinity
     * @param lowerBoundIncluded true if the lower bound should be included
     * @param upperBoundIncluded true if the upper bound should be included
     * @param <T> the type of the range bounds
     * @return an appropriate Range implementation
     */
    public static <T extends Comparable<T>> Range<T> of(T lowerBound, T upperBound,
                                                         boolean lowerBoundIncluded,
                                                         boolean upperBoundIncluded) {
        // Case 1: Both bounds are null - represents all values (-∞, +∞)
        if (lowerBound == null && upperBound == null) {
            Preconditions.checkArgument(lowerBoundIncluded == false && upperBoundIncluded == false);
            return AllRange.getInstance();
        }

        // Case 2: Only lower bound is null - represents (-∞, upperBound) or (-∞, upperBound]
        if (lowerBound == null) {
            Preconditions.checkArgument(lowerBoundIncluded == false);
            if (upperBoundIncluded) {
                return new LERange<>(upperBound);  // (-∞, upperBound]
            } else {
                return new LTRange<>(upperBound);  // (-∞, upperBound)
            }
        }

        // Case 3: Only upper bound is null - represents (lowerBound, +∞) or [lowerBound, +∞)
        if (upperBound == null) {
            Preconditions.checkArgument(upperBoundIncluded == false);
            if (lowerBoundIncluded) {
                return new GERange<>(lowerBound);  // [lowerBound, +∞)
            } else {
                return new GTRange<>(lowerBound);  // (lowerBound, +∞)
            }
        }

        // Case 4: Both bounds are non-null - represents bounded ranges
        if (lowerBoundIncluded && upperBoundIncluded) {
            return new GELERange<>(lowerBound, upperBound);  // [lowerBound, upperBound]
        } else if (lowerBoundIncluded && !upperBoundIncluded) {
            return new GELTRange<>(lowerBound, upperBound);  // [lowerBound, upperBound)
        } else if (!lowerBoundIncluded && upperBoundIncluded) {
            return new GTLERange<>(lowerBound, upperBound);  // (lowerBound, upperBound]
        } else {
            return new GTLTRange<>(lowerBound, upperBound);  // (lowerBound, upperBound)
        }
    }

    /**
     * Creates an AllRange representing all possible values (-∞, +∞).
     *
     * @param <T> the type of the range bounds
     * @return an AllRange instance
     */
    public static <T extends Comparable<T>> Range<T> all() {
        return AllRange.getInstance();
    }

    /**
     * Creates a LTRange representing values less than the upper bound: (-∞, upperBound).
     *
     * @param upperBound the upper bound (excluded)
     * @param <T> the type of the range bounds
     * @return a LTRange instance
     */
    public static <T extends Comparable<T>> Range<T> lt(T upperBound) {
        return new LTRange<>(upperBound);
    }

    /**
     * Creates a LERange representing values less than or equal to the upper bound: (-∞, upperBound].
     *
     * @param upperBound the upper bound (included)
     * @param <T> the type of the range bounds
     * @return a LERange instance
     */
    public static <T extends Comparable<T>> Range<T> le(T upperBound) {
        return new LERange<>(upperBound);
    }

    /**
     * Creates a GTRange representing values greater than the lower bound: (lowerBound, +∞).
     *
     * @param lowerBound the lower bound (excluded)
     * @param <T> the type of the range bounds
     * @return a GTRange instance
     */
    public static <T extends Comparable<T>> Range<T> gt(T lowerBound) {
        return new GTRange<>(lowerBound);
    }

    /**
     * Creates a GERange representing values greater than or equal to the lower bound: [lowerBound, +∞).
     *
     * @param lowerBound the lower bound (included)
     * @param <T> the type of the range bounds
     * @return a GERange instance
     */
    public static <T extends Comparable<T>> Range<T> ge(T lowerBound) {
        return new GERange<>(lowerBound);
    }

    /**
     * Creates a GTLTRange representing an open interval: (lowerBound, upperBound).
     * Both bounds are excluded.
     *
     * @param lowerBound the lower bound (excluded)
     * @param upperBound the upper bound (excluded)
     * @param <T> the type of the range bounds
     * @return a GTLTRange instance
     */
    public static <T extends Comparable<T>> Range<T> gtlt(T lowerBound, T upperBound) {
        return new GTLTRange<>(lowerBound, upperBound);
    }

    /**
     * Creates a GELTRange representing a half-open interval: [lowerBound, upperBound).
     * Lower bound is included, upper bound is excluded.
     *
     * @param lowerBound the lower bound (included)
     * @param upperBound the upper bound (excluded)
     * @param <T> the type of the range bounds
     * @return a GELTRange instance
     */
    public static <T extends Comparable<T>> Range<T> gelt(T lowerBound, T upperBound) {
        return new GELTRange<>(lowerBound, upperBound);
    }

    /**
     * Creates a GTLERange representing a half-open interval: (lowerBound, upperBound].
     * Lower bound is excluded, upper bound is included.
     *
     * @param lowerBound the lower bound (excluded)
     * @param upperBound the upper bound (included)
     * @param <T> the type of the range bounds
     * @return a GTLERange instance
     */
    public static <T extends Comparable<T>> Range<T> gtle(T lowerBound, T upperBound) {
        return new GTLERange<>(lowerBound, upperBound);
    }

    /**
     * Creates a GELERange representing a closed interval: [lowerBound, upperBound].
     * Both bounds are included.
     *
     * @param lowerBound the lower bound (included)
     * @param upperBound the upper bound (included)
     * @param <T> the type of the range bounds
     * @return a GELERange instance
     */
    public static <T extends Comparable<T>> Range<T> gele(T lowerBound, T upperBound) {
        return new GELERange<>(lowerBound, upperBound);
    }

    /**
     * @return the lower bound of the range, or null if the range extends to negative infinity
     */
    public abstract T getLowerBound();

    /**
     * @return the upper bound of the range, or null if the range extends to positive infinity
     */
    public abstract T getUpperBound();

    /**
     * @return true if the lower bound is included in the range (closed), false if excluded (open)
     */
    public abstract boolean isLowerBoundIncluded();

    /**
     * @return true if the upper bound is included in the range (closed), false if excluded (open)
     */
    public abstract boolean isUpperBoundIncluded();

    /**
     * @return true if the lower bound is excluded from the range (open), false if included (closed)
     */
    public boolean isLowerBoundExcluded() {
        return !isLowerBoundIncluded();
    }

    /**
     * @return true if the upper bound is excluded from the range (open), false if included (closed)
     */
    public boolean isUpperBoundExcluded() {
        return !isUpperBoundIncluded();
    }

    /**
     * @return true if the lower bound is negative infinity (null)
     */
    public boolean isMinimum() {
        return getLowerBound() == null;
    }

    /**
     * @return true if the upper bound is positive infinity (null)
     */
    public boolean isMaximum() {
        return getUpperBound() == null;
    }

    /**
     * @return true if this range represents all possible values (-∞, +∞)
     */
    public boolean isAll() {
        return isMinimum() && isMaximum();
    }

    /**
     * Checks if this range represents a single point (e.g., [5, 5]).
     *
     * @return true if both bounds are equal and included
     */
    public boolean isIdentical() {
        return isLowerBoundIncluded() && isUpperBoundIncluded() && Objects.equals(getLowerBound(), getUpperBound());
    }

    /**
     * Checks if this range is entirely less than the given element.
     *
     * @param e the element to compare, must not be null
     * @return true if all values in this range are less than e
     * @throws NullPointerException if e is null
     */
    public boolean isLessThan(T e) {
        Objects.requireNonNull(e, "Element must not be null");

        if (isMaximum()) {
            return false;
        }

        int result = getUpperBound().compareTo(e);
        return result < 0 || (result == 0 && isUpperBoundExcluded());
    }

    /**
     * Checks if this range is entirely greater than the given element.
     *
     * @param e the element to compare, must not be null
     * @return true if all values in this range are greater than e
     * @throws NullPointerException if e is null
     */
    public boolean isGreaterThan(T e) {
        Objects.requireNonNull(e, "Element must not be null");

        if (isMinimum()) {
            return false;
        }

        int result = getLowerBound().compareTo(e);
        return result > 0 || (result == 0 && isLowerBoundExcluded());
    }

    /**
     * Checks if this range contains the given element.
     *
     * @param e the element to check, must not be null
     * @return true if e is within this range
     * @throws NullPointerException if e is null
     */
    public boolean contains(T e) {
        return !(isLessThan(e) || isGreaterThan(e));
    }

    /**
     * Checks if this range is entirely less than another range.
     *
     * @param other the range to compare, must not be null
     * @return true if all values in this range are less than all values in the other range
     * @throws NullPointerException if other is null
     */
    public boolean isLessThan(Range<T> other) {
        Objects.requireNonNull(other, "Range must not be null");

        if (isMaximum() || other.isMinimum()) {
            return false;
        }

        int result = getUpperBound().compareTo(other.getLowerBound());
        return result < 0 || (result == 0 && (isUpperBoundExcluded() || other.isLowerBoundExcluded()));
    }

    /**
     * Checks if this range is entirely greater than another range.
     *
     * @param other the range to compare, must not be null
     * @return true if all values in this range are greater than all values in the other range
     * @throws NullPointerException if other is null
     */
    public boolean isGreaterThan(Range<T> other) {
        Objects.requireNonNull(other, "Range must not be null");

        if (isMinimum() || other.isMaximum()) {
            return false;
        }

        int result = getLowerBound().compareTo(other.getUpperBound());
        return result > 0 || (result == 0 && (isLowerBoundExcluded() || other.isUpperBoundExcluded()));
    }

    /**
     * Checks if this range overlaps with another range.
     *
     * @param other the range to check for overlap, must not be null
     * @return true if the two ranges have any values in common
     * @throws NullPointerException if other is null
     */
    public boolean isOverlapping(Range<T> other) {
        return !(isLessThan(other) || isGreaterThan(other));
    }

    /**
     * Compares this range with another for ordering.
     *
     * <p>Note: This method may return 0 for overlapping ranges that are not equal.
     * It is designed for range ordering, not equality testing. Use {@link #equals(Object)}
     * to test for equality.
     *
     * @param other the range to compare to
     * @return a negative integer, zero, or a positive integer as this range is less than,
     *         overlapping with, or greater than the specified range
     */
    @Override
    public int compareTo(Range<T> other) {
        if (isLessThan(other)) {
            return -1;
        }
        if (isGreaterThan(other)) {
            return 1;
        }
        return 0;
    }

    /**
     * Checks if this range is equal to another object.
     *
     * <p>Two ranges are equal if they have the same bounds (or both null) and
     * the same inclusiveness for both lower and upper bounds.
     *
     * @param object the object to compare with
     * @return true if the ranges are equal
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof Range)) {
            return false;
        }
        Range<?> other = (Range<?>) object;
        return Objects.equals(getLowerBound(), other.getLowerBound())
                && Objects.equals(getUpperBound(), other.getUpperBound())
                && isLowerBoundIncluded() == other.isLowerBoundIncluded()
                && isUpperBoundIncluded() == other.isUpperBoundIncluded();
    }

    /**
     * Returns the hash code for this range.
     *
     * @return the hash code value
     */
    @Override
    public int hashCode() {
        return Objects.hash(getLowerBound(), getUpperBound(),
                isLowerBoundIncluded(), isUpperBoundIncluded());
    }


    /**
     * A range implementation representing all possible values (-∞, +∞).
     * Both bounds are null (infinite) and excluded.
     *
     * <p>This class uses a singleton pattern to ensure all instances share the same object.
     *
     * @param <T> the type of the range bounds
     */
    private static class AllRange<T extends Comparable<T>> extends Range<T> {

        private static final AllRange<?> INSTANCE = new AllRange<>();

        /**
         * Private constructor to prevent external instantiation.
         * Use {@link #getInstance()} to obtain the singleton instance.
         */
        private AllRange() {
        }

        /**
         * Returns the singleton instance of AllRange.
         *
         * @param <T> the type of the range bounds
         * @return the singleton AllRange instance
         */
        @SuppressWarnings("unchecked")
        public static <T extends Comparable<T>> AllRange<T> getInstance() {
            return (AllRange<T>) INSTANCE;
        }

        @Override
        public T getLowerBound() {
            return null;
        }

        @Override
        public T getUpperBound() {
            return null;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return false;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return false;
        }

        @Override
        public String toString() {
            return "(-∞, +∞)";
        }
    }

    /**
     * A range implementation representing values less than an upper bound: (-∞, upperBound).
     * Lower bound is null (negative infinity) and upper bound is excluded.
     *
     * @param <T> the type of the range bounds
     */
    private static class LTRange<T extends Comparable<T>> extends Range<T> {

        private final T upperBound;

        public LTRange(T upperBound) {
            this.upperBound = Objects.requireNonNull(upperBound, "Upper bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return null;
        }

        @Override
        public T getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return false;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return false;
        }

        @Override
        public String toString() {
            return "(-∞, " + upperBound + ")";
        }
    }

    /**
     * A range implementation representing values less than or equal to an upper bound: (-∞, upperBound].
     * Lower bound is null (negative infinity) and upper bound is included.
     *
     * @param <T> the type of the range bounds
     */
    private static class LERange<T extends Comparable<T>> extends Range<T> {

        private final T upperBound;

        public LERange(T upperBound) {
            this.upperBound = Objects.requireNonNull(upperBound, "Upper bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return null;
        }

        @Override
        public T getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return false;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return true;
        }

        @Override
        public String toString() {
            return "(-∞, " + upperBound + "]";
        }
    }

    /**
     * A range implementation representing values greater than a lower bound: (lowerBound, +∞).
     * Lower bound is excluded and upper bound is null (positive infinity).
     *
     * @param <T> the type of the range bounds
     */
    private static class GTRange<T extends Comparable<T>> extends Range<T> {

        private final T lowerBound;

        public GTRange(T lowerBound) {
            this.lowerBound = Objects.requireNonNull(lowerBound, "Lower bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return lowerBound;
        }

        @Override
        public T getUpperBound() {
            return null;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return false;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return false;
        }

        @Override
        public String toString() {
            return "(" + lowerBound + ", +∞)";
        }
    }

    /**
     * A range implementation representing values greater than or equal to a lower bound: [lowerBound, +∞).
     * Lower bound is included and upper bound is null (positive infinity).
     *
     * @param <T> the type of the range bounds
     */
    private static class GERange<T extends Comparable<T>> extends Range<T> {

        private final T lowerBound;

        public GERange(T lowerBound) {
            this.lowerBound = Objects.requireNonNull(lowerBound, "Lower bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return lowerBound;
        }

        @Override
        public T getUpperBound() {
            return null;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return true;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return false;
        }

        @Override
        public String toString() {
            return "[" + lowerBound + ", +∞)";
        }
    }

    /**
     * A range implementation representing values between two bounds: (lowerBound, upperBound).
     * Both bounds are excluded (open interval).
     *
     * @param <T> the type of the range bounds
     */
    private static class GTLTRange<T extends Comparable<T>> extends Range<T> {

        private final T lowerBound;
        private final T upperBound;

        public GTLTRange(T lowerBound, T upperBound) {
            this.lowerBound = Objects.requireNonNull(lowerBound, "Lower bound must not be null");
            this.upperBound = Objects.requireNonNull(upperBound, "Upper bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return lowerBound;
        }

        @Override
        public T getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return false;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return false;
        }

        @Override
        public String toString() {
            return "(" + lowerBound + ", " + upperBound + ")";
        }
    }

    /**
     * A range implementation representing values between two bounds: [lowerBound, upperBound).
     * Lower bound is included, upper bound is excluded (half-open interval).
     *
     * @param <T> the type of the range bounds
     */
    private static class GELTRange<T extends Comparable<T>> extends Range<T> {

        private final T lowerBound;
        private final T upperBound;

        public GELTRange(T lowerBound, T upperBound) {
            this.lowerBound = Objects.requireNonNull(lowerBound, "Lower bound must not be null");
            this.upperBound = Objects.requireNonNull(upperBound, "Upper bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return lowerBound;
        }

        @Override
        public T getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return true;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return false;
        }

        @Override
        public String toString() {
            return "[" + lowerBound + ", " + upperBound + ")";
        }
    }

    /**
     * A range implementation representing values between two bounds: (lowerBound, upperBound].
     * Lower bound is excluded, upper bound is included (half-open interval).
     *
     * @param <T> the type of the range bounds
     */
    private static class GTLERange<T extends Comparable<T>> extends Range<T> {

        private final T lowerBound;
        private final T upperBound;

        public GTLERange(T lowerBound, T upperBound) {
            this.lowerBound = Objects.requireNonNull(lowerBound, "Lower bound must not be null");
            this.upperBound = Objects.requireNonNull(upperBound, "Upper bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return lowerBound;
        }

        @Override
        public T getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return false;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return true;
        }

        @Override
        public String toString() {
            return "(" + lowerBound + ", " + upperBound + "]";
        }
    }

    /**
     * A range implementation representing values between two bounds: [lowerBound, upperBound].
     * Both bounds are included (closed interval).
     *
     * @param <T> the type of the range bounds
     */
    private static class GELERange<T extends Comparable<T>> extends Range<T> {

        private final T lowerBound;
        private final T upperBound;

        public GELERange(T lowerBound, T upperBound) {
            this.lowerBound = Objects.requireNonNull(lowerBound, "Lower bound must not be null");
            this.upperBound = Objects.requireNonNull(upperBound, "Upper bound must not be null");
        }

        @Override
        public T getLowerBound() {
            return lowerBound;
        }

        @Override
        public T getUpperBound() {
            return upperBound;
        }

        @Override
        public boolean isLowerBoundIncluded() {
            return true;
        }

        @Override
        public boolean isUpperBoundIncluded() {
            return true;
        }

        @Override
        public String toString() {
            return "[" + lowerBound + ", " + upperBound + "]";
        }
    }
}