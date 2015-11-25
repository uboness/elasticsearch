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

package org.elasticsearch.plugin.ingest.transport.simulate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;

public class SimulateExecutionService {

    private static final String THREAD_POOL_NAME = ThreadPool.Names.MANAGEMENT;

    private final ThreadPool threadPool;

    @Inject
    public SimulateExecutionService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    SimulateDocumentResult executeItem(Pipeline pipeline, IngestDocument ingestDocument) {
        try {
            pipeline.execute(ingestDocument);
            return new SimulateDocumentSimpleResult(ingestDocument);
        } catch (Exception e) {
            return new SimulateDocumentSimpleResult(e);
        }
    }

    // uboness: do we really need two methods here? both executeItem method can be folded into one
    // that accepts a "boolean verbose" arg. Also, can we rename to `executeDocument` (why item?)
    SimulateDocumentVerboseResult executeVerboseItem(Pipeline pipeline, IngestDocument ingestDocument) {
        List<SimulateProcessorResult> processorResultList = new ArrayList<>();
        IngestDocument currentIngestDocument = new IngestDocument(ingestDocument);
        for (int i = 0; i < pipeline.getProcessors().size(); i++) {
            Processor processor = pipeline.getProcessors().get(i);
            String processorId = "processor[" + processor.getType() + "]-" + i;

            try {
                processor.execute(currentIngestDocument);
                processorResultList.add(new SimulateProcessorResult(processorId, currentIngestDocument));
            } catch (Exception e) {
                processorResultList.add(new SimulateProcessorResult(processorId, e));
            }

            currentIngestDocument = new IngestDocument(currentIngestDocument);
        }
        return new SimulateDocumentVerboseResult(processorResultList);
    }

    public void execute(ParsedSimulateRequest request, ActionListener<SimulatePipelineResponse> listener) {
        threadPool.executor(THREAD_POOL_NAME).execute(new Runnable() {
            @Override
            public void run() {
                List<SimulateDocumentResult> responses = new ArrayList<>();
                for (IngestDocument ingestDocument : request.getDocuments()) {
                    if (request.isVerbose()) {
                        responses.add(executeVerboseItem(request.getPipeline(), ingestDocument));
                    } else {
                        responses.add(executeItem(request.getPipeline(), ingestDocument));
                    }
                }
                listener.onResponse(new SimulatePipelineResponse(request.getPipeline().getId(), request.isVerbose(), responses));
            }
        });
    }
}