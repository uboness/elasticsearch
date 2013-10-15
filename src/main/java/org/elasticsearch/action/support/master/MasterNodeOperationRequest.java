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

package org.elasticsearch.action.support.master;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * A based request for master based operation.
 */
public abstract class MasterNodeOperationRequest<T extends MasterNodeOperationRequest> extends ActionRequest<T> {

    public static TimeValue DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    protected TimeValue masterNodeTimeout = DEFAULT_MASTER_NODE_TIMEOUT;

    protected String annotation;

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final T masterNodeTimeout(TimeValue timeout) {
        this.masterNodeTimeout = timeout;
        return (T) this;
    }

    /**
     * A timeout value in case the master has not been discovered yet or disconnected.
     */
    public final T masterNodeTimeout(String timeout) {
        return masterNodeTimeout(TimeValue.parseTimeValue(timeout, null));
    }

    public final TimeValue masterNodeTimeout() {
        return this.masterNodeTimeout;
    }

    /**
     * Annotates the request with additional information about it which will in turn be logged in the log file. It is highly recommended
     * to annotate any request that changes the cluster state as it makes it easier to trace the cluster state in the logs.
     */
    public final T annotation(String annotation) {
        this.annotation = annotation;
        return (T) this;
    }

    public String annotation() {
        return annotation;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        masterNodeTimeout = TimeValue.readTimeValue(in);
        if (in.getVersion().onOrAfter(Version.V_0_90_6)) {
            annotation = in.readOptionalString();
        } else {
            annotation = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        masterNodeTimeout.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_0_90_6)) {
            out.writeOptionalString(annotation);
        }
    }
}
