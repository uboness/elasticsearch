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

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.common.unit.TimeValue;

/**
 */
public abstract class MasterNodeOperationRequestBuilder<Request extends MasterNodeOperationRequest<Request>, Response extends ActionResponse, RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response, RequestBuilder> {

    protected MasterNodeOperationRequestBuilder(InternalGenericClient client, Request request) {
        super(client, request);
    }

    /**
     * Sets the master node timeout in case the master has not yet been discovered.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setMasterNodeTimeout(TimeValue timeout) {
        request.masterNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Sets the master node timeout in case the master has not yet been discovered.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setMasterNodeTimeout(String timeout) {
        request.masterNodeTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Annotates the request with additional information about it which will in turn be logged in the log file. It is highly recommended
     * to annotate any request that changes the cluster state as it makes it easier to trace the cluster state in the logs.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setAnnotation(String annotation) {
        request.annotation(annotation);
        return (RequestBuilder) this;
    }

}
