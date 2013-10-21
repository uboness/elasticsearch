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

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 *
 */
public class ClearIndicesCacheRequestBuilder extends BroadcastOperationRequestBuilder<ClearIndicesCacheRequest, ClearIndicesCacheResponse, ClearIndicesCacheRequestBuilder> {

    public ClearIndicesCacheRequestBuilder(IndicesAdminClient indicesClient) {
        super((InternalIndicesAdminClient) indicesClient, new ClearIndicesCacheRequest());
    }

    public ClearIndicesCacheRequestBuilder setFilterCache(boolean filterCache) {
        request.filterCache(filterCache);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setFieldDataCache(boolean fieldDataCache) {
        request.fieldDataCache(fieldDataCache);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setFilterKeys(String... filterKeys) {
        request.filterKeys(filterKeys);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setIdCache(boolean idCache) {
        request.idCache(idCache);
        return this;
    }

    public ClearIndicesCacheRequestBuilder setAnnotation(String annotation) {
        request.annotation(annotation);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<ClearIndicesCacheResponse> listener) {
        ((IndicesAdminClient) client).clearCache(request, listener);
    }
}
