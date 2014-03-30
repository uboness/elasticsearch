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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;

/**
 *
 */
public abstract class InternalMultiBucketsAggregation extends InternalAggregation implements MultiBucketsAggregation {

    protected TrackingInfo info;

    protected InternalMultiBucketsAggregation() {} // for serialization

    protected InternalMultiBucketsAggregation(String name, TrackingInfo info) {
        super(name);
        this.info = info;
    }

    @Override
    public TrackingInfo getBucketsInfo() {
        return info;
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        internalReadFrom(in);
        if (in.getVersion().onOrAfter(TrackingInfo.MIN_SUPPORTED_VERSION)) {
            info = in.readBoolean() ? TrackingInfo.readFrom(in) : null;
        }
    }

    protected abstract void internalReadFrom(StreamInput in) throws IOException;

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        internalWriteTo(out);
        if (out.getVersion().onOrAfter(TrackingInfo.MIN_SUPPORTED_VERSION)) {
            if (info != null) {
                out.writeBoolean(true);
                TrackingInfo.writeTo(info, out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    protected abstract void internalWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        if (info != null) {
            info.toXContent(builder, params);
        }
        bucketsToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract void bucketsToXContent(XContentBuilder builder, Params params) throws IOException;
}
