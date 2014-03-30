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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.bucket.TrackingInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class UnmappedTerms extends InternalTerms {

    public static final Type TYPE = new Type("terms", "umterms");

    private static final Collection<Bucket> BUCKETS = Collections.emptyList();
    private static final Map<String, Bucket> BUCKETS_MAP = Collections.emptyMap();

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public UnmappedTerms readResult(StreamInput in) throws IOException {
            UnmappedTerms buckets = new UnmappedTerms();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    UnmappedTerms() {} // for serialization

    public UnmappedTerms(String name, InternalOrder order, int requiredSize, long minDocCount) {
        super(name, TrackingInfo.EMPTY, order, requiredSize, minDocCount, BUCKETS);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<Bucket> readBucketsFrom(StreamInput in) throws IOException {
        return BUCKETS;
    }

    @Override
    public void writeBucketsTo(StreamOutput out, Collection<Bucket> buckets) throws IOException {
        assert buckets == BUCKETS;
    }

    @Override
    public void bucketsToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS).endArray();
    }

}
