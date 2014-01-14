package org.elasticsearch.search.aggregations.metrics.percentile.qdigest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.aggregations.metrics.percentile.PercentilesEstimator;

import java.io.IOException;
import java.util.Map;

public class QDigest extends PercentilesEstimator {

    public final static byte ID = 1;

    public ObjectArray<QDigestState> states;
    private final double compression;

    public QDigest(double[] percents, double compression, long estimatedBucketCount) {
        super(percents);
        this.compression = compression;
        states = BigArrays.newObjectArray(estimatedBucketCount);
    }

    public void offer(double value, long bucketOrd) {
        QDigestState state = states.get(bucketOrd);
        if (state == null) {
            state = new QDigestState(compression);
            states.set(bucketOrd, state);
        }
        state.offer((long) value);
    }

    public long ramBytesUsed() {
        //todo impl
        return -1;
    }

    @Override
    public PercentilesEstimator.Flyweight flyweight(long bucketOrd) {
        return new Flyweight(percents, states.get(bucketOrd));
    }

    @Override
    public PercentilesEstimator.Flyweight emptyFlyweight() {
        return new Flyweight(percents, null);
    }

    public static class Flyweight extends PercentilesEstimator.Flyweight<QDigest, Flyweight> {

        public QDigestState state;

        public Flyweight() {} // for serialization

        public Flyweight(double[] percents, QDigestState state) {
            super(percents);
            this.state = state;
        }

        @Override
        protected byte id() {
            return ID;
        }

        @Override
        public double estimate(int index) {
            return state == null || state.isEmpty() ? Double.NaN : state.getQuantile(percents[index] / 100);
        }

        @Override
        public Merger merger(int estimatedMerges) {
            return new Merger();
        }

        public static Flyweight read(StreamInput in) throws IOException {
            Flyweight flyweight = new Flyweight();
            flyweight.readFrom(in);
            return flyweight;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.percents = new double[in.readInt()];
            for (int i = 0; i < percents.length; i++) {
                percents[i] = in.readDouble();
            }
            state = QDigestState.read(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(percents.length);
            for (int i = 0 ; i < percents.length; ++i) {
                out.writeDouble(percents[i]);
            }
            QDigestState.write(state, out);
        }

        public class Merger implements PercentilesEstimator.Flyweight.Merger<QDigest, Flyweight> {

            private Flyweight merged;

            @Override
            public void add(Flyweight flyweight) {
                if (merged == null || merged.state == null) {
                    merged = flyweight;
                    return;
                }

                if (flyweight.state == null || flyweight.state.isEmpty()) {
                    return;
                }
                merged.state = QDigestState.unionOf(merged.state, flyweight.state);
            }

            @Override
            public Flyweight merge() {
                return merged;
            }
        }
    }

    public static class Factory implements PercentilesEstimator.Factory<QDigest> {

        private final double compression;

        public Factory(Map<String, Object> settings) {
            double compression = 100;
            if (settings != null) {
                Double value = (Double) settings.get("compression");
                if (value != null) {
                    compression = value;
                }
            }
            this.compression = compression;
        }

        public QDigest create(double[] percents, long estimatedBucketCount) {
            return new QDigest(percents, compression, estimatedBucketCount);
        }
    }

}