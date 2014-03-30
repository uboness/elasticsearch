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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.TrackingValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

/**
 *
 */
public class TrackingInfo implements ToXContent {

    public final static Version MIN_SUPPORTED_VERSION = Version.V_1_2_0;

    public static final XContentBuilderString MISSING = new XContentBuilderString("missing");
    public static final XContentBuilderString DOC_VALUE_COUNT = new XContentBuilderString("doc_value_count");

    public static final TrackingInfo EMPTY = new TrackingInfo(0, 0, 0);

    private long missingDocCount;
    private long docCount;
    private long docValueCount;

    public TrackingInfo(long missingDocCount, long docCount, long docValueCount) {
        this.missingDocCount = missingDocCount;
        this.docCount = docCount;
        this.docValueCount = docValueCount;
    }

    public long getMissingDocCount() {
        return missingDocCount;
    }

    public long getDocCount() {
        return docCount;
    }

    public long getDocValueCount() {
        return docValueCount;
    }

    public void add(TrackingInfo info) {
        if (info != null) {
            this.missingDocCount += info.missingDocCount;
        }
    }

    public static TrackingInfo resolve(ValuesSource valuesSource) {
        if (valuesSource instanceof TrackingValuesSource) {
            TrackingValuesSource tracking = (TrackingValuesSource) valuesSource;
            return new TrackingInfo(tracking.missingCount(), tracking.docCount(), tracking.docValueCount());
        }
        return null;
    }

    public static void writeTo(TrackingInfo info, StreamOutput out) throws IOException {
        out.writeVLong(info.missingDocCount);
        out.writeVLong(info.docCount);
        out.writeVLong(info.docValueCount);
    }

    public static TrackingInfo readFrom(StreamInput in) throws IOException {
        return new TrackingInfo(in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MISSING, missingDocCount);
        builder.field(InternalAggregation.CommonFields.DOC_COUNT, docCount);
        builder.field(DOC_VALUE_COUNT, docValueCount);
        return builder;
    }
}
