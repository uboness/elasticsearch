/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.terms.missing;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.InternalTermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class InternalMissingFieldFacet extends InternalTermsFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray("mtTerms");

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readTermsFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    long missing;

    InternalMissingFieldFacet() {
    }

    public InternalMissingFieldFacet(String name, long missing) {
        super(name);
        this.missing = missing;
    }

    @Override
    public List<Entry> getEntries() {
        return Collections.emptyList();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) getEntries().iterator();
    }

    @Override
    public long getMissingCount() {
        return this.missing;
    }

    @Override
    public long getTotalCount() {
        return 0;
    }

    @Override
    public long getOtherCount() {
        return 0;
    }

    @Override
    public Facet reduce(List<Facet> facets) {
        Facet first = facets.get(0);
        if (facets.size() == 1) {
            return first;
        }
        int missing = 0;
        for (Facet facet : facets) {
            if (!(facet instanceof InternalMissingFieldFacet)) {
                return ((InternalFacet) facet).reduce(facets);
            }
            InternalMissingFieldFacet missingFacet = (InternalMissingFieldFacet) facet;
            missing += missingFacet.missing;
        }

        ((InternalMissingFieldFacet) first).missing = missing;
        return first;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString MISSING = new XContentBuilderString("missing");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, TermsFacet.TYPE);
        builder.field(Fields.MISSING, missing);
        builder.field(Fields.TOTAL, 0);
        builder.endObject();
        return builder;
    }

    public static InternalMissingFieldFacet readTermsFacet(StreamInput in) throws IOException {
        InternalMissingFieldFacet facet = new InternalMissingFieldFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        missing = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(missing);
    }
}