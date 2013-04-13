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

package org.elasticsearch.transport.debug;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.netty.NettyTransport;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 */
public class DebugTransport extends AbstractLifecycleComponent<Transport> implements Transport {

    private final Transport transport;
    private TransportServiceAdapter adapter;
    private final Set<DiscoveryNode> disconnectedNodes;

    @Inject
    public DebugTransport(Settings settings, Transport transport, NodeSettingsService nodeSettingsService) {
        super(settings);
        this.transport = transport;
        disconnectedNodes = new CopyOnWriteArraySet<DiscoveryNode>();
        nodeSettingsService.addListener(new ApplySettings());
    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {

        }
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        if (transport instanceof LifecycleComponent) {
            ((LifecycleComponent) transport).start();
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if (transport instanceof LifecycleComponent) {
            ((LifecycleComponent) transport).stop();
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        if (transport instanceof LifecycleComponent) {
            ((LifecycleComponent) transport).close();
        }
    }

    @Override
    public void transportServiceAdapter(TransportServiceAdapter service) {
        transport.transportServiceAdapter(service);
        this.adapter = service;
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    @Override
    public TransportAddress[] addressesFromString(String address) throws Exception {
        return transport.addressesFromString(address);
    }

    @Override
    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return transport.addressSupported(address);
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return !disconnectedNodes.contains(node) && transport.nodeConnected(node);
    }

    @Override
    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        if (disconnectedNodes.contains(node)) {
            throw new ConnectTransportException(node, "Cannot connect to node [" + node + "]. reason: simulated partition");
        }
        transport.connectToNode(node);
    }

    @Override
    public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
        if (disconnectedNodes.contains(node)) {
            throw new ConnectTransportException(node, "Cannot light connect to node [" + node + "]. reason: simulated partition");
        }
        transport.connectToNodeLight(node);
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        if (disconnectedNodes.contains(node)) {
           logger.info("Node already disconnected due to simulated partition");
        }
        transport.disconnectFromNode(node);
    }

    @Override
    public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
        if (disconnectedNodes.contains(node)) {
            throw new SimulatedPartitionException("Cannot send request [" + action + "] to node [" + node + "] due to simulated partition");
        }
        transport.sendRequest(node, requestId, action, request, options);
    }

    @Override
    public long serverOpen() {
        return transport.serverOpen();
    }

}
