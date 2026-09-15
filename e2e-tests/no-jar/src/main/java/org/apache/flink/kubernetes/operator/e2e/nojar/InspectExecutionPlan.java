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

import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.InstantiationUtil;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Read-only inspection of a fixture-owned HA execution-plan file, not a checkpoint metadata file.
 *
 * <p>Run with the same runtime/application classpath: java -cp '/opt/flink/lib/*'
 * org.apache.flink.kubernetes.operator.e2e.nojar.InspectExecutionPlan FILE [EXPECTED_JAR_COUNT].
 * Input is Java serialization: use only trusted files produced by this test's disposable cluster.
 */
public final class InspectExecutionPlan {
    private InspectExecutionPlan() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException("Expected FILE [EXPECTED_JAR_COUNT]");
        }
        final ExecutionPlan plan = read(Path.of(args[0]));
        System.out.printf(
                Locale.ROOT,
                "PERSISTED_GRAPH type=%s job=%s userJars=%d userJarBlobKeys=%d jars=%s blobKeys=%s classpaths=%s%n",
                plan.getClass().getSimpleName(),
                plan.getJobID(),
                plan.getUserJars().size(),
                plan.getUserJarBlobKeys().size(),
                plan.getUserJars(),
                plan.getUserJarBlobKeys(),
                plan.getClasspaths());
        if (args.length == 2) {
            final int expected = Integer.parseInt(args[1]);
            if (expected < 0
                    || plan.getUserJars().size() != expected
                    || plan.getUserJarBlobKeys().size() != expected) {
                throw new IllegalStateException(
                        "Unexpected persisted JAR metadata; expected " + expected);
            }
        }
    }

    static ExecutionPlan read(Path path) throws Exception {
        try (InputStream input = Files.newInputStream(path)) {
            final Object state =
                    InstantiationUtil.deserializeObject(
                            input, Thread.currentThread().getContextClassLoader());
            if (!(state instanceof ExecutionPlan plan)) {
                throw new IllegalArgumentException("Not an HA ExecutionPlan: " + path);
            }
            return plan;
        }
    }
}
