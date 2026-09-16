/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.e2e.nojar;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Observes the actual submitted graph, never a newly materialized JobGraph. */
final class SubmissionEvidence implements JobListener {
    private static final Logger LOG = LoggerFactory.getLogger(SubmissionEvidence.class);

    private final StreamGraph graph;

    SubmissionEvidence(StreamGraph graph) {
        this.graph = graph;
    }

    @Override
    public void onJobSubmitted(JobClient jobClient, Throwable throwable) {
        if (throwable != null) {
            LOG.error("GRAPH_METADATA submission_failed", throwable);
            return;
        }
        // EmbeddedExecutor can recover a persisted graph without uploading this newly built graph.
        // That fresh, unsubmitted graph must not be reported as zero-BLOB evidence.
        if (!isSubmittedGraph(graph, jobClient.getJobID())) {
            LOG.info("GRAPH_METADATA phase=recovered_not_observed job={}", jobClient.getJobID());
            return;
        }
        // In Flink 2.3 EmbeddedExecutor uploads files into this same StreamGraph before the
        // submission future completes and StreamExecutionEnvironment invokes JobListener.
        LOG.info(
                "GRAPH_METADATA phase=post_upload job={} userJars={} userJarBlobKeys={} jars={} blobKeys={}",
                graph.getJobID(),
                graph.getUserJars().size(),
                graph.getUserJarBlobKeys().size(),
                graph.getUserJars(),
                graph.getUserJarBlobKeys());
    }

    static boolean isSubmittedGraph(StreamGraph graph, JobID jobId) {
        try {
            // Flink 2.3 checks its serialization holder for null in this accessor. EmbeddedExecutor
            // initializes that holder after file upload, but not when recovering a persisted graph.
            return graph.getSerializedExecutionConfig() != null && graph.getJobID().equals(jobId);
        } catch (NullPointerException notSerialized) {
            return false;
        }
    }

    @Override
    public void onJobExecuted(JobExecutionResult jobExecutionResult, Throwable throwable) {
        if (throwable != null) {
            LOG.error("FIXTURE_EXECUTION_FAILED", throwable);
        }
    }
}
