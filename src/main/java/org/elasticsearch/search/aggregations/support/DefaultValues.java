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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DefaultValues {

    private final Object[] values;

    public DefaultValues(Object[] values) {
        this.values = values;
    }

    public Object[] values() {
        return values;
    }

    public BytesRef[] bytesValues() {
        if (values == null) {
            return null;
        }
        BytesRef[] vals = new BytesRef[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof BytesRef) {
                vals[i] = (BytesRef) values[i];
            } else if (values[i] instanceof CharSequence) {
                vals[i] =  new BytesRef((CharSequence) values[i]);
            } else if (values[i] instanceof GeoPoint) {
                GeoPoint point = (GeoPoint) values[i];
                vals[i] = new BytesRef(GeoHashUtils.encode(point.lat(), point.lon()));
            }
        }
        return vals;
    }

    public long[] longValues() {
        if (values == null) {
            return null;
        }
        long[] vals = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof Number) {
                vals[i] = ((Number) values[i]).longValue();
            } else if (values[i] instanceof GeoPoint) {
                GeoPoint point = (GeoPoint) values[i];
                vals[i] = GeoHashUtils.encodeAsLong(point.lat(), point.lon(), GeoHashUtils.PRECISION);
            } else {
                vals[i] = Long.parseLong(values[i].toString());
            }
        }
        return vals;
    }

    public double[] doubleValues() {
        if (values == null) {
            return null;
        }
        double[] vals = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof Number) {
                vals[i] = ((Number) values[i]).doubleValue();
            } else if (values[i] instanceof GeoPoint) {
                GeoPoint point = (GeoPoint) values[i];
                vals[i] = GeoHashUtils.encodeAsLong(point.lat(), point.lon(), GeoHashUtils.PRECISION);
            } else {
                vals[i] = Double.parseDouble(values[i].toString());
            }
        }
        return vals;
    }

    public GeoPoint[] geoPointValues() {
        if (values == null) {
            return null;
        }
        GeoPoint[] vals = new GeoPoint[values.length];
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof GeoPoint) {
                vals[i] = (GeoPoint) values[i];
            } else if (double[].class.isInstance(values[i])) {
                double[] lonLat = (double[]) values[i];
                if (lonLat.length != 2) {
                    throw new AggregationExecutionException("Cannot parse double array " + Arrays.toString(lonLat) + " as geo_point");
                }
                vals[i] = new GeoPoint(lonLat[1], lonLat[0]);
            } else if (values[i] instanceof String) {
                vals[i] = GeoPoint.parseFromLatLon((String) values[i]);
            } else {
                throw new AggregationExecutionException("Cannot convert [" + values[i] + "] to geo_point");
            }
        }
        return vals;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final List<Object> values = new ArrayList<>();

        private Builder() {}

        public Builder value(Object value) {
            values.add(value);
            return this;
        }

        public DefaultValues build() {
            return new DefaultValues(values.toArray(new Object[values.size()]));
        }

    }
}
