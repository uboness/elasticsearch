/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.LongValues;

/**
 *
 */
public interface TrackingValuesSource {

    long missingCount();

    long docCount();

    long docValueCount();

    static class Bytes extends ValuesSource.Bytes implements TrackingValuesSource, ReaderContextAware {

        private final ValuesSource delegate;
        private final Counter missingCounter;
        private final Counter docCounter;
        private final Counter docValueCounter;

        private TrackingBytesValues bytesValues;

        /**
         * TODO The assumption here, is that if {@code missingValues != null}, then it doesn't hold duplicates (is this
         *      a fair assumption after all the field data doesn't hold duplicates as well?)
         *
         * @param delegate  The FieldDataSource this source is wrapping
         */
        public Bytes(ValuesSource delegate) {
            this.delegate = delegate;
            this.missingCounter = new Counter();
            this.docCounter = new Counter();
            this.docValueCounter = new Counter();
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            if (delegate instanceof ReaderContextAware) {
                ((ReaderContextAware) delegate).setNextReader(reader);
            }
            bytesValues = null; // resetting
        }

        @Override
        public BytesValues bytesValues() {
            if (bytesValues == null) {
                bytesValues = new TrackingBytesValues(delegate.bytesValues(), missingCounter, docCounter, docValueCounter);
            }
            return bytesValues;
        }

        @Override
        public MetaData metaData() {
            return delegate.metaData();
        }

        public long missingCount() {
            return missingCounter.count;
        }

        @Override
        public long docCount() {
            return docCounter.count;
        }

        @Override
        public long docValueCount() {
            return docValueCounter.count;
        }

        private static class TrackingBytesValues extends BytesValues {

            private final BytesValues values;
            private final Counter missingCounter;
            private final Counter docCounter;
            private final Counter docValueCounter;

            private TrackingBytesValues(BytesValues values, Counter missingCounter, Counter docCounter, Counter docValueCounter) {
                super(values.isMultiValued());
                this.values = values;
                this.missingCounter = missingCounter;
                this.docCounter = docCounter;
                this.docValueCounter = docValueCounter;
            }

            @Override
            public int setDocument(int docId) {
                docCounter.count++;
                int valueCount = values.setDocument(docId);
                docValueCounter.count += valueCount;
                if (valueCount == 0) {
                    missingCounter.count++;
                }
                return valueCount;
            }

            @Override
            public BytesRef copyShared() {
                return values.copyShared();
            }

            @Override
            public int currentValueHash() {
                return values.currentValueHash();
            }

            @Override
            public BytesRef nextValue() {
                return values.nextValue();
            }
        }
    }

    public static class Numeric extends ValuesSource.Numeric implements ReaderContextAware, TrackingValuesSource {

        private final ValuesSource.Numeric delegate;
        private final Counter missingCounter;
        private final Counter docCounter;
        private final Counter docValueCounter;

        private TrackingValuesSource.Bytes.TrackingBytesValues bytesValues;
        private MissingTrackingDoubleValues doubleValues;
        private MissingTrackingLongValues longValues;

        /**
         * TODO The assumption here, is that if {@code missingValues != null}, then it doesn't hold duplicates (is this
         *      a fair assumption after all the field data doesn't hold duplicates as well?)
         *
         * @param delegate  The FieldDataSource this source is wrapping
         */
        public Numeric(ValuesSource.Numeric delegate) {
            this.delegate = delegate;
            this.missingCounter = new Counter();
            this.docCounter = new Counter();
            this.docValueCounter = new Counter();
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            if (delegate instanceof ReaderContextAware) {
                ((ReaderContextAware) delegate).setNextReader(reader);
            }
            bytesValues = null;
            longValues = null;
            doubleValues = null;
        }

        @Override
        public BytesValues bytesValues() {
            if (bytesValues == null) {
                bytesValues = new TrackingValuesSource.Bytes.TrackingBytesValues(delegate.bytesValues(), missingCounter, docCounter, docValueCounter);
            }
            return bytesValues;
        }

        @Override
        public boolean isFloatingPoint() {
            return delegate.isFloatingPoint();
        }

        @Override
        public LongValues longValues() {
            if (longValues == null) {
                longValues = new MissingTrackingLongValues(delegate.longValues(), missingCounter, docCounter, docValueCounter);
            }
            return longValues;
        }

        @Override
        public DoubleValues doubleValues() {
            if (doubleValues == null) {
                doubleValues = new MissingTrackingDoubleValues(delegate.doubleValues(), missingCounter, docCounter, docValueCounter);
            }
            return doubleValues;
        }

        @Override
        public MetaData metaData() {
            return delegate.metaData();
        }

        public long missingCount() {
            return missingCounter.count;
        }

        @Override
        public long docCount() {
            return docCounter.count;
        }

        @Override
        public long docValueCount() {
            return docValueCounter.count;
        }

        private static class MissingTrackingDoubleValues extends DoubleValues {

            private final DoubleValues values;
            private final Counter missingCounter;
            private final Counter docCounter;
            private final Counter docValueCounter;

            private MissingTrackingDoubleValues(DoubleValues values, Counter missingCounter, Counter docCounter, Counter docValueCounter) {
                super(values.isMultiValued());
                this.values = values;
                this.missingCounter = missingCounter;
                this.docCounter = docCounter;
                this.docValueCounter = docValueCounter;
            }

            @Override
            public int setDocument(int docId) {
                docCounter.count++;
                int valueCount = values.setDocument(docId);
                docValueCounter.count += valueCount;
                if (valueCount == 0) {
                    missingCounter.count++;
                }
                return valueCount;
            }

            @Override
            public double nextValue() {
                return values.nextValue();
            }
        }

        private static class MissingTrackingLongValues extends LongValues {

            private final LongValues values;
            private final Counter missingCounter;
            private final Counter docCounter;
            private final Counter docValueCounter;

            private MissingTrackingLongValues(LongValues values, Counter missingCounter, Counter docCounter, Counter docValueCounter) {
                super(values.isMultiValued());
                this.values = values;
                this.missingCounter = missingCounter;
                this.docCounter = docCounter;
                this.docValueCounter = docValueCounter;
            }

            @Override
            public int setDocument(int docId) {
                docCounter.count++;
                int valueCount = values.setDocument(docId);
                docValueCounter.count += valueCount;
                if (valueCount == 0) {
                    missingCounter.count++;
                }
                return valueCount;
            }

            @Override
            public long nextValue() {
                return values.nextValue();
            }
        }
    }

    static class GeoPoint extends ValuesSource.GeoPoint implements TrackingValuesSource, ReaderContextAware {

        private final ValuesSource.GeoPoint delegate;
        private final Counter missingCounter;
        private final Counter docCounter;
        private final Counter docValueCounter;

        private TrackingValuesSource.Bytes.TrackingBytesValues bytesValues;
        private MissingTrackingGeoPointValues geoPointValues;

        /**
         * TODO The assumption here, is that if {@code missingValues != null}, then it doesn't hold duplicates (is this
         *      a fair assumption after all the field data doesn't hold duplicates as well?)
         *
         * @param delegate  The FieldDataSource this source is wrapping
         */
        public GeoPoint(ValuesSource.GeoPoint delegate) {
            this.delegate = delegate;
            this.missingCounter = new Counter();
            this.docCounter = new Counter();
            this.docValueCounter = new Counter();
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            if (delegate instanceof ReaderContextAware) {
                ((ReaderContextAware) delegate).setNextReader(reader);
            }
            bytesValues = null; // resetting
        }

        @Override
        public BytesValues bytesValues() {
            if (bytesValues == null) {
                bytesValues = new TrackingValuesSource.Bytes.TrackingBytesValues(delegate.bytesValues(), missingCounter, docCounter, docValueCounter);
            }
            return bytesValues;
        }

        @Override
        public GeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = new MissingTrackingGeoPointValues(delegate.geoPointValues(), missingCounter, docCounter, docValueCounter);
            }
            return geoPointValues;
        }

        @Override
        public MetaData metaData() {
            return delegate.metaData();
        }

        public long missingCount() {
            return missingCounter.count;
        }

        @Override
        public long docCount() {
            return docCounter.count;
        }

        @Override
        public long docValueCount() {
            return docValueCounter.count;
        }

        private static class MissingTrackingGeoPointValues extends GeoPointValues {

            private final GeoPointValues values;
            private final Counter missingCounter;
            private final Counter docCounter;
            private final Counter docValueCounter;

            private MissingTrackingGeoPointValues(GeoPointValues values, Counter missingCounter, Counter docCounter, Counter docValueCounter) {
                super(values.isMultiValued());
                this.values = values;
                this.missingCounter = missingCounter;
                this.docCounter = docCounter;
                this.docValueCounter = docValueCounter;
            }

            @Override
            public int setDocument(int docId) {
                docCounter.count++;
                int valueCount = values.setDocument(docId);
                docValueCounter.count += valueCount;
                if (valueCount == 0) {
                    missingCounter.count++;
                }
                return valueCount;
            }

            @Override
            public org.elasticsearch.common.geo.GeoPoint nextValue() {
                return values.nextValue();
            }
        }
    }

    static class Counter {
        long count;
    }
}
