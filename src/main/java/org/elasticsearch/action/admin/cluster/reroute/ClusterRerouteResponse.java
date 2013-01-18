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

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.AllocationExplanation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class ClusterRerouteResponse extends ActionResponse {

    private ClusterState state;
    private AllocationExplanation explanation;

    ClusterRerouteResponse() {

    }

    ClusterRerouteResponse(ClusterState state, AllocationExplanation explanation) {
        this.state = state;
        this.explanation = explanation;
    }

    public ClusterState state() {
        return this.state;
    }

    public ClusterState getState() {
        return this.state;
    }

    public AllocationExplanation explanation() {
        return this.explanation;
    }

    public AllocationExplanation getExplanation() {
        return this.explanation;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        state = ClusterState.Builder.readFrom(in, null);
        explanation = AllocationExplanation.readAllocationExplanation(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        ClusterState.Builder.writeTo(state, out);
        explanation.writeTo(out);
    }
}
