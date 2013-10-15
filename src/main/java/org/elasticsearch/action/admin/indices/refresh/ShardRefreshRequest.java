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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
class ShardRefreshRequest extends BroadcastShardOperationRequest {

    private boolean force = true;

    private String annotation;

    ShardRefreshRequest() {
    }

    public ShardRefreshRequest(String index, int shardId, RefreshRequest request) {
        super(index, shardId, request);
        force = request.force();
        annotation = request.annotation();
    }

    public boolean force() {
        return force;
    }

    public String annotation() {
        return annotation;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        force = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_0_90_6)) {
            annotation = in.readOptionalString();
        } else {
            annotation = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(force);
        if (out.getVersion().onOrAfter(Version.V_0_90_6)) {
            out.writeOptionalString(annotation);
        }
    }
}
