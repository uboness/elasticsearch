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

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.cluster.routing.allocation.AllocationExplanation;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 */
public class ClusterRerouteRequest extends MasterNodeOperationRequest<ClusterRerouteRequest> {

    public static enum Explain {

        NONE(AllocationExplanation.Level.INFO),
        BASIC(AllocationExplanation.Level.DEBUG),
        DETAILED(AllocationExplanation.Level.TRACE);

        private final AllocationExplanation.Level level;

        private Explain(AllocationExplanation.Level level) {
            this.level = level;
        }

        AllocationExplanation.Level explanationLevel() {
            return level;
        }
    }

    AllocationCommands commands = new AllocationCommands();
    boolean dryRun;
    private Explain explain = Explain.BASIC;

    public ClusterRerouteRequest() {
    }

    /**
     * Adds allocation commands to be applied to the cluster. Note, can be empty, in which case
     * will simply run a simple "reroute".
     */
    public ClusterRerouteRequest add(AllocationCommand... commands) {
        this.commands.add(commands);
        return this;
    }

    /**
     * Sets a dry run flag (defaults to <tt>false</tt>) allowing to run the commands without
     * actually applying them to the cluster state, and getting the resulting cluster state back.
     */
    public ClusterRerouteRequest dryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public boolean dryRun() {
        return this.dryRun;
    }

    /**
     * Sets the allocation explanation level
     *
     * @param explain The explanation level.
     */
    public ClusterRerouteRequest explain(Explain explain) {
        this.explain = explain;
        return this;
    }

    public Explain explain() {
        return explain;
    }

    /**
     * Sets the source for the request.
     */
    public ClusterRerouteRequest source(BytesReference source) throws Exception {
        XContentParser parser = XContentHelper.createParser(source);
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("commands".equals(currentFieldName)) {
                        this.commands = AllocationCommands.fromXContent(parser);
                    } else {
                        throw new ElasticSearchParseException("failed to parse reroute request, got start array with wrong field name [" + currentFieldName + "]");
                    }
                } else if (token.isValue()) {
                    if ("dry_run".equals(currentFieldName) || "dryRun".equals(currentFieldName)) {
                        dryRun = parser.booleanValue();
                    } else {
                        throw new ElasticSearchParseException("failed to parse reroute request, got value with wrong field name [" + currentFieldName + "]");
                    }
                }
            }
        } finally {
            parser.close();
        }
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        commands = AllocationCommands.readFrom(in);
        dryRun = in.readBoolean();
        explain = Explain.valueOf(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        AllocationCommands.writeTo(commands, out);
        out.writeBoolean(dryRun);
        out.writeString(explain.name());
    }
}
