package org.elasticsearch.search.aggregations.metrics.percentile.frugal;

import com.carrotsearch.hppc.DoubleArrayList;
import jsr166y.ThreadLocalRandom;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.metrics.percentile.PercentilesEstimator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class Frugal extends PercentilesEstimator {

    public final static byte ID = 0;

    private final Random rand;

    private DoubleArray mins;
    private DoubleArray maxes;
    public ObjectArray<double[]> estimates;  // Current estimate of percentile
    private ObjectArray<int[]> steps;        // Current step value for frugal-2u
    private ObjectArray<Bits> signs;   // Direction of last movement

    /**
     * Instantiate a new FrugalProvider
     * <p>
     * This class implements the Frugal-2U algorithm for streaming quantiles.  See
     * http://dx.doi.org/10.1007/978-3-642-40273-9_7 for original paper, and
     * http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/
     * for "layman" explanation.
     * <p>
     * Frugal-2U maintains a probabilistic estimate of the requested percentile, using
     * minimal memory to do so.  Error can grow as high as 50% under certain circumstances,
     * particularly during cold-start and when the stream drifts suddenly
     *
     * @param percents how many intervals to calculate quantiles for
     */
    public Frugal(final double[] percents, long estimatedBucketCount) {
        super(percents);
        mins = BigArrays.newDoubleArray(estimatedBucketCount);
        mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        maxes = BigArrays.newDoubleArray(estimatedBucketCount);
        maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
        steps = BigArrays.newObjectArray(estimatedBucketCount);
        steps.fill(0, steps.size(), new ObjectArray.Filler<int[]>() {
            @Override
            public int[] objectFor(long index) {
                int[] vals = new int[percents.length];
                Arrays.fill(vals, 1);
                return vals;
            }
        });
        signs = BigArrays.newObjectArray(estimatedBucketCount);
        signs.fill(0, steps.size(), new ObjectArray.Filler<Bits>() {
            @Override
            public Bits objectFor(long index) {
                return Bits.create(percents.length);
            }
        });
        estimates = BigArrays.newObjectArray(estimatedBucketCount);
        this.rand = ThreadLocalRandom.current();
    }

    /**
     * Offer a new value to the streaming percentile algo.  May modify the current
     * estimate
     *
     * @param value Value to stream
     */
    public void offer(double value, long bucketOrd) {

        double[] estimates = null;
        if (this.estimates.size() > bucketOrd) {
            estimates = this.estimates.get(bucketOrd);
        } else {
            long overSize = BigArrays.overSize(bucketOrd + 1);
            this.estimates = BigArrays.resize(this.estimates, overSize);
            long from = mins.size();
            mins = BigArrays.resize(mins, overSize);
            mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
            from = maxes.size();
            maxes = BigArrays.resize(maxes, overSize);
            maxes.fill(from, maxes.size(), Double.NEGATIVE_INFINITY);
            from = steps.size();
            steps = BigArrays.resize(steps, overSize);
            steps.fill(from, steps.size(), new ObjectArray.Filler<int[]>() {
                @Override
                public int[] objectFor(long index) {
                    int[] vals = new int[percents.length];
                    Arrays.fill(vals, 1);
                    return vals;
                }
            });
            from = signs.size();
            signs = BigArrays.resize(signs, overSize);
            signs.fill(from, steps.size(), new ObjectArray.Filler<Bits>() {
                @Override
                public Bits objectFor(long index) {
                    return Bits.create(percents.length);
                }
            });
        }
        estimates = this.estimates.get(bucketOrd);
        // Set estimate to first value in stream...helps to avoid fully cold starts

        if (estimates == null) {

            double[] bucketEstimates = new double[percents.length];
            this.estimates.set(bucketOrd, bucketEstimates);
            Arrays.fill(bucketEstimates, value);
            mins.set(bucketOrd, value);
            maxes.set(bucketOrd, value);
            return;
        }

        mins.set(bucketOrd, Math.min(value, mins.get(bucketOrd)));
        maxes.set(bucketOrd, Math.max(value, maxes.get(bucketOrd)));

        final double randomValue = rand.nextDouble() * 100;
        for (int i = 0 ; i < percents.length; i++) {
            offerTo(estimates, i, value, randomValue, bucketOrd);
        }
    }

    private void offerTo(double[] estimates, int index, double value, double randomValue, long bucketOrd) {
        double percent = this.percents[index];

        if (percent == 0 || percent == 100) {
            // we calculate those separately
            return;
        }

        int[] steps = this.steps.get(bucketOrd);
        Bits signs = this.signs.get(bucketOrd);


        /**
         * Movements in the same direction are rewarded with a boost to step, and
         * a big change to estimate. Movement in opposite direction gets negative
         * step boost but still a small boost to estimate
         */

        if (value > estimates[index] && randomValue > (100.0d - percent)) {
            steps[index] += signs.get(index) ? 1 : -1;

            if (steps[index] > 0) {
                estimates[index] += steps[index];
            } else {
                ++estimates[index];
            }

            signs.set(index);

            //If we overshot, reduce step and reset estimate
            if (estimates[index] > value) {
                steps[index] += (value - estimates[index]);
                estimates[index] = value;
            }

        } else if (value < estimates[index] && randomValue < (100.0d - percent)) {
            steps[index] += signs.get(index) ? -1 : 1;

            if (steps[index] > 0) {
                estimates[index] -= steps[index];
            } else {
                --estimates[index];
            }

            signs.clear(index);

            //If we overshot, reduce step and reset estimate
            if (estimates[index] < value) {
                steps[index] += (estimates[index] - value);
                estimates[index] = value;
            }
        }

        // Smooth out oscillations
        if ((estimates[index] - value) * (signs.get(index) ? 1 : -1)  < 0 && steps[index] > 1) {
            steps[index] = 1;
        }

        // Prevent step from growing more negative than necessary
        if (steps[index] <= -Integer.MAX_VALUE + 1000) {
            steps[index] = -Integer.MAX_VALUE + 1000;
        }
    }

    @Override
    public PercentilesEstimator.Flyweight flyweight(long bucketOrd) {
        return new Flyweight(percents, estimates.get(bucketOrd), mins.get(bucketOrd), maxes.get(bucketOrd));
    }

    @Override
    public PercentilesEstimator.Flyweight emptyFlyweight() {
        return new Flyweight(percents, null, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    }

    @Override
    public long ramBytesUsed() {
        //todo impl
        return -1;
    }

    public static class Flyweight extends PercentilesEstimator.Flyweight<Frugal, Flyweight> {

        public double[] estimates;
        private double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;

        public Flyweight() {} // for serialization

        public Flyweight(double[] percents, double[] estimates, double min, double max) {
            super(percents);
            this.estimates = estimates;
            this.min = min;
            this.max = max;
        }

        @Override
        protected byte id() {
            return 0;
        }

        @Override
        public double estimate(int index) {
            if (estimates == null) {
                return Double.NaN;
            }
            if (percents[index] == 0) {
                return min;
            } else if (percents[index] == 100) {
                return max;
            } else {
                return Math.max(Math.min(estimates[index], max), min);
            }
        }

        @Override
        public Merger merger(int estimatedMerges) {
            return new Merger(estimatedMerges);
        }

        public static Flyweight readNewFrom(StreamInput in) throws IOException {
            Flyweight flyweight = new Flyweight();
            flyweight.readFrom(in);
            return flyweight;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.percents = new double[in.readInt()];
            this.estimates = in.readBoolean() ? new double[this.percents.length] : null;

            if (estimates != null) {
                min = in.readDouble();
                max = in.readDouble();
            }

            for (int i = 0 ; i < percents.length; ++i) {
                percents[i] = in.readDouble();
                if (estimates != null) {
                    estimates[i] = in.readDouble();
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(percents.length);
            out.writeBoolean(estimates != null);
            if (estimates != null) {
                out.writeDouble(min);
                out.writeDouble(max);
            }
            for (int i = 0 ; i < percents.length; ++i) {
                out.writeDouble(percents[i]);
                if (estimates != null) {
                    out.writeDouble(estimates[i]);
                }
            }
        }

        /**
         * Responsible for merging multiple frugal estimators. Merging is accomplished by taking the median for
         * each percentile.  More accurate than simply averaging, though probably slower.
         */
        private class Merger implements PercentilesEstimator.Flyweight.Merger<Frugal, Flyweight> {

            private final int expectedMerges;
            private DoubleArrayList merging;

            private Merger(int expectedMerges) {
                this.expectedMerges = expectedMerges;
            }

            @Override
            public void add(Flyweight flyweight) {

                if (flyweight.estimates == null) {
                    return;
                }

                min = Math.min(min, flyweight.min);
                max = Math.max(max, flyweight.max);

                if (merging == null) {
                    merging = new DoubleArrayList(expectedMerges * percents.length);
                }

                for (int i = 0; i < percents.length; ++i) {
                    merging.add(flyweight.estimate(i));
                }
            }

            private double weightedValue(DoubleArrayList list, double index) {
                assert index <= list.size() - 1;
                final int intIndex = (int) index;
                final double d = index - intIndex;
                if (d == 0) {
                    return list.get(intIndex);
                } else {
                    return (1 - d) * list.get(intIndex) + d * list.get(intIndex + 1);
                }
            }

            @Override
            public Flyweight merge() {
                if (merging != null) {
                    if (estimates == null) {
                        estimates = new double[percents.length];
                    }
                    CollectionUtils.sort(merging);
                    final int numMerges = merging.size() / percents.length;
                    for (int i = 0; i < percents.length; ++i) {
                        estimates[i] = weightedValue(merging, numMerges * i + (percents[i] / 100 * (numMerges - 1)));
                    }
                }
                return Flyweight.this;
            }

        }
    }

    public static class Factory implements PercentilesEstimator.Factory<Frugal> {

        public Frugal create(double[] percents, long estimatedBucketCount) {
            return new Frugal(percents, estimatedBucketCount);
        }

    }

    public static abstract class Bits {

        public static Bits create(long size) {
            return size < 65 ? new LongBits() : new UnboundedBits(size);
        }

        abstract void set(int index);

        abstract boolean get(int index);

        abstract void clear(int index);

        abstract void clearAll();

        abstract void setAll();

        private static class UnboundedBits extends Bits {

            private OpenBitSet bitset;

            private UnboundedBits(long size) {
                bitset = new OpenBitSet(size);
            }

            @Override
            public void set(int index) {
                bitset.fastSet(index);
            }

            @Override
            public boolean get(int index) {
                return bitset.fastGet(index);
            }

            @Override
            public void clear(int index) {
                bitset.fastClear(index);
            }

            @Override
            public void clearAll() {
                bitset.clear(0, bitset.size());
            }

            @Override
            public void setAll() {
                bitset.set(0, bitset.size());
            }
        }

        private static class LongBits extends Bits {

            public long bits;

            private LongBits() {}

            public void set(int index) {
                bits |= (0x8000L >>> index);
            }

            @Override
            public boolean get(int index) {
                return (bits & (0x8000L >>> index)) != 0;
            }

            public void clear(int index) {
                bits &= ~(0x8000L >>> index);
            }

            public void clearAll() {
                bits = 0x0L;
            }

            public void setAll() {
                bits = 0xffff;
            }
        }
    }
}