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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.streaming.api.graph.ExecutionPlan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InspectExecutionPlanTest {
    @TempDir Path directory;

    @Test
    void readsJobGraphAndChecksExpectedCount() throws Exception {
        final JobGraph graph = new JobGraph("persisted-test");
        final Path file = persist(graph);
        final ExecutionPlan decoded = InspectExecutionPlan.read(file);
        assertThat(decoded).isInstanceOf(JobGraph.class);
        assertThat(decoded.getJobID()).isEqualTo(graph.getJobID());
        InspectExecutionPlan.main(new String[] {file.toString(), "0"});
        assertThatThrownBy(() -> InspectExecutionPlan.main(new String[] {file.toString(), "1"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unexpected persisted JAR metadata");
    }

    @Test
    void rejectsNonExecutionPlanFiles() throws Exception {
        final Path file = persist("not an execution plan");
        assertThatThrownBy(() -> InspectExecutionPlan.read(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not an HA ExecutionPlan");
    }

    private Path persist(Serializable value) throws Exception {
        new FileSystemStateStorageHelper<Serializable>(
                        new org.apache.flink.core.fs.Path(directory.toUri()), "submitted-")
                .store(value);
        try (Stream<Path> files = Files.list(directory)) {
            final List<Path> plans = files.toList();
            assertThat(plans).hasSize(1);
            return plans.get(0);
        }
    }
}
