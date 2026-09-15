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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Flink 2.3 upload and listener path; only the external Dispatcher RPC boundary is faked. */
class SubmissionEvidenceTest {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(30)
    void observesUploadedMetadataInActualJobListener(boolean submitJar) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        final Path jar = directory.resolve("control.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.finish();
        }
        if (submitJar) {
            configuration.set(PipelineOptions.JARS, List.of(jar.toUri().toString()));
        }
        final AtomicReference<StreamGraph> submittedGraph = new AtomicReference<>();
        final AtomicBoolean listenerCalled = new AtomicBoolean();
        try (BlobServer server =
                new BlobServer(
                        configuration, directory.resolve("blobs").toFile(), new VoidBlobStore())) {
            server.start();
            final DispatcherGateway gateway =
                    (DispatcherGateway)
                            Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class<?>[] {DispatcherGateway.class},
                                    (proxy, method, args) -> {
                                        switch (method.getName()) {
                                            case "getBlobServerPort":
                                                return CompletableFuture.completedFuture(
                                                        server.getPort());
                                            case "getBlobServerAddress":
                                                return CompletableFuture.completedFuture(
                                                        InetAddress.getLoopbackAddress());
                                            case "submitJob":
                                                submittedGraph.set((StreamGraph) args[0]);
                                                new FileSystemStateStorageHelper<StreamGraph>(
                                                                new org.apache.flink.core.fs.Path(
                                                                        directory
                                                                                .resolve("ha")
                                                                                .toUri()),
                                                                "submitted-")
                                                        .store(submittedGraph.get());
                                                return CompletableFuture.completedFuture(
                                                        Acknowledge.get());
                                            case "requestJobStatus":
                                                return CompletableFuture.completedFuture(
                                                        JobStatus.RUNNING);
                                            default:
                                                throw new UnsupportedOperationException(
                                                        method.getName());
                                        }
                                    });
            final EmbeddedExecutor executor =
                    new EmbeddedExecutor(
                            new ArrayList<>(),
                            gateway,
                            configuration,
                            (jobId, loader) -> jobClient(jobId));
            try (StreamExecutionEnvironment env =
                    new StreamExecutionEnvironment(
                            loader(executor), configuration, getClass().getClassLoader())) {
                env.setParallelism(1);
                env.fromData(1L).sinkTo(new DiscardingSink<>());
                final StreamGraph graph = env.getStreamGraph();
                assertThat(SubmissionEvidence.isSubmittedGraph(graph, new JobID())).isFalse();
                env.registerJobListener(new SubmissionEvidence(graph));
                env.registerJobListener(
                        new JobListener() {
                            @Override
                            public void onJobSubmitted(JobClient jobClient, Throwable throwable) {
                                assertThat(throwable).isNull();
                                assertThat(submittedGraph.get()).isSameAs(graph);
                                assertThat(
                                                SubmissionEvidence.isSubmittedGraph(
                                                        graph, jobClient.getJobID()))
                                        .isTrue();
                                assertThat(graph.getUserJars()).hasSize(submitJar ? 1 : 0);
                                assertThat(graph.getUserJarBlobKeys()).hasSize(submitJar ? 1 : 0);
                                listenerCalled.set(true);
                            }

                            @Override
                            public void onJobExecuted(
                                    JobExecutionResult result, Throwable throwable) {}
                        });
                env.executeAsync(graph);
                assertThat(listenerCalled).isTrue();
                try (Stream<Path> files = Files.list(directory.resolve("ha"))) {
                    final List<Path> plans = files.toList();
                    assertThat(plans).hasSize(1);
                    final ExecutionPlan persisted = InspectExecutionPlan.read(plans.get(0));
                    assertThat(persisted).isInstanceOf(StreamGraph.class);
                    assertThat(persisted.getJobID()).isEqualTo(graph.getJobID());
                    assertThat(persisted.getUserJars())
                            .containsExactlyElementsOf(graph.getUserJars());
                    assertThat(persisted.getUserJarBlobKeys())
                            .containsExactlyElementsOf(graph.getUserJarBlobKeys());
                }
                if (submitJar) {
                    assertThat(server.getFile(graph.getJobID(), graph.getUserJarBlobKeys().get(0)))
                            .hasSameBinaryContentAs(jar.toFile());
                }
            }
        }
    }

    @Test
    void unsubmittedRecoveryGraphIsNotZeroBlobEvidence() throws Exception {
        try (StreamExecutionEnvironment env = new StreamExecutionEnvironment()) {
            env.fromData(1L).sinkTo(new DiscardingSink<>());
            final StreamGraph graph = env.getStreamGraph();
            graph.setJobId(new JobID());
            assertThat(SubmissionEvidence.isSubmittedGraph(graph, graph.getJobID())).isFalse();
            assertThat(SubmissionEvidence.isSubmittedGraph(graph, new JobID())).isFalse();
        }
    }

    private static JobClient jobClient(JobID jobId) {
        return (JobClient)
                Proxy.newProxyInstance(
                        JobClient.class.getClassLoader(),
                        new Class<?>[] {JobClient.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getJobID")) {
                                return jobId;
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static PipelineExecutorServiceLoader loader(EmbeddedExecutor executor) {
        return new PipelineExecutorServiceLoader() {
            @Override
            public PipelineExecutorFactory getExecutorFactory(Configuration configuration) {
                return new PipelineExecutorFactory() {
                    @Override
                    public String getName() {
                        return EmbeddedExecutor.NAME;
                    }

                    @Override
                    public boolean isCompatibleWith(Configuration configuration) {
                        return true;
                    }

                    @Override
                    public PipelineExecutor getExecutor(Configuration configuration) {
                        return executor;
                    }
                };
            }

            @Override
            public Stream<String> getExecutorNames() {
                return Stream.of(EmbeddedExecutor.NAME);
            }
        };
    }
}
