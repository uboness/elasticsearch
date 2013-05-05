/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facet.range;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class InternalRangeFacet extends InternalFacet implements RangeFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray("range");

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readRangeFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    Entry[] entries;
    long missing;
    long other;

    InternalRangeFacet() {
    }

    public InternalRangeFacet(String name, Entry[] entries, long missing, long other) {
        super(name);
        this.entries = entries;
        this.missing = missing;
        this.other = other;
    }

    @Override
    public String getType() {
        return RangeFacet.TYPE;
    }

    @Override
    public List<Entry> getEntries() {
        return ImmutableList.copyOf(entries);
    }

    @Override
    public Iterator<Entry> iterator() {
        return getEntries().iterator();
    }

    @Override
    public Facet reduce(List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalRangeFacet agg = null;
        long missing = 0;
        for (Facet facet : facets) {
            InternalRangeFacet rangeFacet = (InternalRangeFacet) facet;
            if (agg == null) {
                agg = rangeFacet;
            } else {
                agg.missing += rangeFacet.missing;
                agg.other += rangeFacet.other;
                for (int i = 0; i < rangeFacet.entries.length; i++) {
                    RangeFacet.Entry aggEntry = agg.entries[i];
                    RangeFacet.Entry currentEntry = rangeFacet.entries[i];
                    aggEntry.count += currentEntry.count;
                    aggEntry.totalCount += currentEntry.totalCount;
                    aggEntry.total += currentEntry.total;
                    if (currentEntry.min < aggEntry.min) {
                        aggEntry.min = currentEntry.min;
                    }
                    if (currentEntry.max > aggEntry.max) {
                        aggEntry.max = currentEntry.max;
                    }
                }
            }
        }
        return agg;
    }

    public static InternalRangeFacet readRangeFacet(StreamInput in) throws IOException {
        InternalRangeFacet facet = new InternalRangeFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        missing = in.readVLong();
        other = in.readVLong();
        entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            Entry entry = new Entry();
            entry.from = in.readDouble();
            entry.to = in.readDouble();
            if (in.readBoolean()) {
                entry.fromAsString = in.readString();
            }
            if (in.readBoolean()) {
                entry.toAsString = in.readString();
            }
            entry.count = in.readVLong();
            entry.totalCount = in.readVLong();
            entry.total = in.readDouble();
            entry.min = in.readDouble();
            entry.max = in.readDouble();
            entry.missing = in.readVLong();
            entries[i] = entry;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(missing);
        out.writeVLong(other);
        out.writeVInt(entries.length);
        for (Entry entry : entries) {
            out.writeDouble(entry.from);
            out.writeDouble(entry.to);
            if (entry.fromAsString == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(entry.fromAsString);
            }
            if (entry.toAsString == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(entry.toAsString);
            }
            out.writeVLong(entry.count);
            out.writeVLong(entry.totalCount);
            out.writeDouble(entry.total);
            out.writeDouble(entry.min);
            out.writeDouble(entry.max);
            out.writeVLong(entry.missing);
        }
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString MISSING = new XContentBuilderString("missing");
        static final XContentBuilderString OTHER = new XContentBuilderString("other");
        static final XContentBuilderString RANGES = new XContentBuilderString("ranges");
        static final XContentBuilderString FROM = new XContentBuilderString("from");
        static final XContentBuilderString FROM_STR = new XContentBuilderString("from_str");
        static final XContentBuilderString TO = new XContentBuilderString("to");
        static final XContentBuilderString TO_STR = new XContentBuilderString("to_str");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, "range");
        builder.field(Fields.MISSING, missing);
        builder.field(Fields.OTHER, other);
        builder.startArray(Fields.RANGES);
        for (Entry entry : entries) {
            builder.startObject();
            if (!Double.isInfinite(entry.from)) {
                builder.field(Fields.FROM, entry.from);
            }
            if (entry.fromAsString != null) {
                builder.field(Fields.FROM_STR, entry.fromAsString);
            }
            if (!Double.isInfinite(entry.to)) {
                builder.field(Fields.TO, entry.to);
            }
            if (entry.toAsString != null) {
                builder.field(Fields.TO_STR, entry.toAsString);
            }
            builder.field(Fields.COUNT, entry.getCount());
            // only output min and max if there are actually documents matching this range...
            if (entry.getTotalCount() > 0) {
                builder.field(Fields.MIN, entry.getMin());
                builder.field(Fields.MAX, entry.getMax());
            }
            builder.field(Fields.TOTAL_COUNT, entry.getTotalCount());
            builder.field(Fields.TOTAL, entry.getTotal());
            builder.field(Fields.MEAN, entry.getMean());
            if (entry.missing > 0) {
                builder.field(Fields.MISSING, entry.missing);
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
