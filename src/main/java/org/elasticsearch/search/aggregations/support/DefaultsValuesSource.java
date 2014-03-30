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
public interface DefaultsValuesSource {

    static class Bytes extends ValuesSource.Bytes implements DefaultsValuesSource, ReaderContextAware {

        private final ValuesSource delegate;
        private final BytesRef[] values;
        private final MetaData metaData;
        private InternalBytesValues bytesValues;

        /**
         * TODO The assumption here, is that if {@code missingValues != null}, then it doesn't hold duplicates (is this
         *      a fair assumption after all the field data doesn't hold duplicates as well?)
         *
         * @param delegate  The FieldDataSource this source is wrapping
         * @param values    The default values
         */
        public Bytes(ValuesSource delegate, DefaultValues values) {
            this.delegate = delegate;
            this.values = values.bytesValues();
            if (this.values == null) {
                metaData = delegate.metaData();
            } else {
                metaData = MetaData.builder()
                        .maxAtomicUniqueValuesCount(Math.max(delegate.metaData().maxAtomicUniqueValuesCount, this.values.length))
                        .uniqueness(delegate.metaData().uniqueness.and(MetaData.Uniqueness.UNIQUE))
                        .multiValued(delegate.metaData().multiValued || this.values.length > 1)
                        .build();
            }
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
                bytesValues = new InternalBytesValues(delegate.bytesValues(), values);
            }
            return bytesValues;
        }

        @Override
        public MetaData metaData() {
            return metaData;
        }

        private static class InternalBytesValues extends BytesValues {

            private final BytesValues values;
            private final BytesRef[] defaultValues;
            private int cursor = -1;

            private InternalBytesValues(BytesValues values, BytesRef[] defaultValues) {
                super(values.isMultiValued());
                this.values = values;
                this.defaultValues = defaultValues;
            }

            @Override
            public int setDocument(int docId) {
                int valueCount = values.setDocument(docId);
                if (valueCount != 0) {
                    cursor = -1;
                    return valueCount;
                }
                if (defaultValues == null) {
                    return 0;
                }
                cursor = 0;
                return defaultValues.length;
            }

            @Override
            public BytesRef copyShared() {
                if (cursor > -1) {
                    assert cursor > 0 : "nextValue should be called at least once before this copyShared";
                    return BytesRef.deepCopyOf(defaultValues[cursor-1]);
                }
                return values.copyShared();
            }

            @Override
            public int currentValueHash() {
                if (cursor > -1) {
                    assert cursor > 0 : "nextValue should be called at least once before this currentValueHash";
                    return defaultValues[cursor-1].hashCode();
                }
                return values.currentValueHash();
            }

            @Override
            public BytesRef nextValue() {
                if (cursor > -1) {
                    return defaultValues[cursor++];
                }
                return values.nextValue();
            }
        }
    }

    public static class Numeric extends ValuesSource.Numeric implements ReaderContextAware, DefaultsValuesSource {

        private final ValuesSource.Numeric delegate;
        private final double[] defaultValues;
        private final BytesRef[] defaultBytesValues;
        private final MetaData metaData;
        private DefaultsValuesSource.Bytes.InternalBytesValues bytesValues;
        private InternalDoubleValues doubleValues;
        private InternalLongValues longValues;

        /**
         * TODO The assumption here, is that if {@code missingValues != null}, then it doesn't hold duplicates (is this
         *      a fair assumption after all the field data doesn't hold duplicates as well?)
         *
         * @param delegate      The FieldDataSource this source is wrapping
         * @param defaultValues1       The default missing values that should be considered for those documents that miss values
         */
        public Numeric(ValuesSource.Numeric delegate, DefaultValues defaultValues1) {
            this.delegate = delegate;
            this.defaultValues = defaultValues1.doubleValues();
            this.defaultBytesValues = defaultValues1.bytesValues(); // should probably be lazy
            if (defaultValues == null) {
                metaData = delegate.metaData();
            } else {
                metaData = MetaData.builder()
                        .maxAtomicUniqueValuesCount(Math.max(delegate.metaData().maxAtomicUniqueValuesCount, defaultValues.length))
                        .uniqueness(delegate.metaData().uniqueness)
                        .multiValued(delegate.metaData().multiValued || defaultValues.length > 1)
                        .build();
            }
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
                bytesValues = new DefaultsValuesSource.Bytes.InternalBytesValues(delegate.bytesValues(), defaultBytesValues);
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
                longValues = new InternalLongValues(delegate.longValues(), defaultValues);
            }
            return longValues;
        }

        @Override
        public DoubleValues doubleValues() {
            if (doubleValues == null) {
                doubleValues = new InternalDoubleValues(delegate.doubleValues(), defaultValues);
            }
            return doubleValues;
        }

        @Override
        public MetaData metaData() {
            return metaData;
        }

        private static class InternalDoubleValues extends DoubleValues {

            private final DoubleValues values;
            private final double[] defaultValues;
            private int cursor = -1;

            private InternalDoubleValues(DoubleValues values, double[] defaultValues) {
                super(values.isMultiValued());
                this.values = values;
                this.defaultValues = defaultValues;
            }

            @Override
            public int setDocument(int docId) {
                int valueCount = values.setDocument(docId);
                if (valueCount != 0) {
                    return valueCount;
                }
                if (defaultValues == null) {
                    return 0;
                }
                cursor = 0;
                return defaultValues.length;
            }

            @Override
            public double nextValue() {
                if (cursor > -1) {
                    return defaultValues[cursor++];
                }
                return values.nextValue();
            }
        }

        private static class InternalLongValues extends LongValues {

            private final LongValues values;
            private final double[] defaultValues;
            private int cursor = -1;

            private InternalLongValues(LongValues values, double[] defaultValues) {
                super(values.isMultiValued());
                this.values = values;
                this.defaultValues = defaultValues;
            }

            @Override
            public int setDocument(int docId) {
                int valueCount = values.setDocument(docId);
                if (valueCount != 0) {
                    return valueCount;
                }
                if (defaultValues == null) {
                    return 0;
                }
                cursor = 0;
                return defaultValues.length;
            }

            @Override
            public long nextValue() {
                if (cursor > -1) {
                    return (long) defaultValues[cursor++];
                }
                return values.nextValue();
            }
        }
    }

    static class GeoPoint extends ValuesSource.GeoPoint implements DefaultsValuesSource, ReaderContextAware {

        private final ValuesSource.GeoPoint delegate;
        private final org.elasticsearch.common.geo.GeoPoint[] defaultValues;
        private final BytesRef[] defaultBytesValues;
        private final MetaData metaData;
        private DefaultsValuesSource.Bytes.InternalBytesValues bytesValues;
        private InternalGeoPointValues geoPointValues;

        /**
         * TODO The assumption here, is that if {@code missingValues != null}, then it doesn't hold duplicates (is this
         *      a fair assumption after all the field data doesn't hold duplicates as well?)
         *
         * @param delegate  The FieldDataSource this source is wrapping
         * @param defaultValues   The missing configuration (potentially holds the default values for missing docs)
         */
        public GeoPoint(ValuesSource.GeoPoint delegate, DefaultValues defaultValues) {
            this.delegate = delegate;
            this.defaultValues = defaultValues.geoPointValues();
            this.defaultBytesValues = defaultValues.bytesValues();
            if (this.defaultValues == null) {
                metaData = delegate.metaData();
            } else {
                metaData = MetaData.builder()
                        .maxAtomicUniqueValuesCount(Math.max(delegate.metaData().maxAtomicUniqueValuesCount, this.defaultValues.length))
                        .uniqueness(delegate.metaData().uniqueness.and(MetaData.Uniqueness.UNIQUE))
                        .multiValued(delegate.metaData().multiValued || this.defaultValues.length > 1)
                        .build();
            }
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
                bytesValues = new DefaultsValuesSource.Bytes.InternalBytesValues(delegate.bytesValues(), defaultBytesValues);
            }
            return bytesValues;
        }

        @Override
        public GeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = new InternalGeoPointValues(delegate.geoPointValues(), defaultValues);
            }
            return geoPointValues;
        }

        @Override
        public MetaData metaData() {
            return metaData;
        }

        private static class InternalGeoPointValues extends GeoPointValues {

            private final GeoPointValues values;
            private final org.elasticsearch.common.geo.GeoPoint[] defaultValues;
            private int cursor = -1;

            private InternalGeoPointValues(GeoPointValues values, org.elasticsearch.common.geo.GeoPoint[] defaultValues) {
                super(values.isMultiValued());
                this.values = values;
                this.defaultValues = defaultValues;
            }

            @Override
            public int setDocument(int docId) {
                int valueCount = values.setDocument(docId);
                if (valueCount != 0) {
                    return valueCount;
                }
                if (defaultValues == null) {
                    return 0;
                }
                cursor = 0;
                return defaultValues.length;
            }

            @Override
            public org.elasticsearch.common.geo.GeoPoint nextValue() {
                if (cursor > -1) {
                    return defaultValues[cursor++];
                }
                return values.nextValue();
            }
        }
    }
}
