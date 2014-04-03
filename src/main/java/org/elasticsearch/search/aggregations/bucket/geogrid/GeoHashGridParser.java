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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.TrackingInfo;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;

/**
 * Aggregates Geo information into cells determined by geohashes of a given precision.
 * WARNING - for high-precision geohashes it may prove necessary to use a {@link GeoBoundingBoxFilterBuilder}
 * aggregation to focus in on a smaller area to avoid generating too many buckets and using too much RAM
 */
public class GeoHashGridParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalGeoHashGrid.TYPE.name();
    }

    public static final int DEFAULT_PRECISION = 5;
    public static final int DEFAULT_MAX_NUM_CELLS = 10000;

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        String field = null;
        int precision = DEFAULT_PRECISION;
        int requiredSize = DEFAULT_MAX_NUM_CELLS;
        int shardSize = -1;


        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("precision".equals(currentFieldName)) {
                    precision = parser.intValue();
                } else if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                }

            }
        }

        if (shardSize == 0) {
            shardSize = Integer.MAX_VALUE;
        }

        if (requiredSize == 0) {
            requiredSize = Integer.MAX_VALUE;
        }

        if (shardSize < 0) {
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting            
            shardSize = BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());
        }

        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        ValuesSourceConfig<ValuesSource.GeoPoint> config = new ValuesSourceConfig<>(ValuesSource.GeoPoint.class);
        if (field == null) {
            return new GeoGridFactory(aggregationName, config, precision, requiredSize, shardSize);
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return new GeoGridFactory(aggregationName, config, precision, requiredSize, shardSize);
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        return new GeoGridFactory(aggregationName, config, precision, requiredSize, shardSize);
    }


    private static class GeoGridFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private int precision;
        private int requiredSize;
        private int shardSize;

        public GeoGridFactory(String name, ValuesSourceConfig<ValuesSource.GeoPoint> valueSourceConfig,
                              int precision, int requiredSize, int shardSize) {
            super(name, InternalGeoHashGrid.TYPE.name(), valueSourceConfig);
            this.precision = precision;
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            final InternalAggregation aggregation = new InternalGeoHashGrid(name, TrackingInfo.EMPTY, requiredSize, Collections.<InternalGeoHashGrid.Bucket>emptyList());
            return new NonCollectingAggregator(name, aggregationContext, parent) {
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator create(final ValuesSource.GeoPoint valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            final CellValues cellIdValues = new CellValues(valuesSource, precision);
            ValuesSource.Numeric cellIdSource = new CellIdSource(cellIdValues, valuesSource.metaData());
            if (cellIdSource.metaData().multiValued()) {
                // we need to wrap to ensure uniqueness
                cellIdSource = new ValuesSource.Numeric.SortedAndUnique(cellIdSource);
            }
            return new GeoHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize, aggregationContext, parent);

        }

        private static class CellValues extends LongValues {

            private ValuesSource.GeoPoint geoPointValues;
            private GeoPointValues geoValues;
            private int precision;

            protected CellValues(ValuesSource.GeoPoint geoPointValues, int precision) {
                super(true);
                this.geoPointValues = geoPointValues;
                this.precision = precision;
            }

            @Override
            public int setDocument(int docId) {
                geoValues = geoPointValues.geoPointValues();
                return geoValues.setDocument(docId);
            }

            @Override
            public long nextValue() {
                GeoPoint target = geoValues.nextValue();
                return GeoHashUtils.encodeAsLong(target.getLat(), target.getLon(), precision);
            }

        }

        private static class CellIdSource extends ValuesSource.Numeric {
            private final LongValues values;
            private MetaData metaData;

            public CellIdSource(LongValues values, MetaData delegate) {
                this.values = values;
                //different GeoPoints could map to the same or different geohash cells.
                this.metaData = MetaData.builder(delegate).uniqueness(MetaData.Uniqueness.UNKNOWN).build();
            }

            @Override
            public boolean isFloatingPoint() {
                return false;
            }

            @Override
            public LongValues longValues() {
                return values;
            }

            @Override
            public DoubleValues doubleValues() {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytesValues bytesValues() {
                throw new UnsupportedOperationException();
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

        }
    }

}