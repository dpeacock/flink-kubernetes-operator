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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.kubernetes.operator.e2e.nojar.helper.DeterministicHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NoJarTestJobTest {
    @Test
    void helperLoadsResourceAndComputesKnownValues() throws Exception {
        final DeterministicHelper helper = new DeterministicHelper();
        assertThat(helper.transform(1)).isEqualTo(24);
        assertThat(helper.transform(100)).isEqualTo(1707);
        assertThatThrownBy(() -> helper.transform(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void restoresExactlyOneCounter() {
        assertThat(NoJarTestJob.restoreCount(List.of(), false, false)).isZero();
        assertThat(NoJarTestJob.restoreCount(List.of(42L), true, true)).isEqualTo(42);
        assertThat(NoJarTestJob.restoreCount(List.of(0L), true, false)).isZero();
    }

    @Test
    void rejectsStatelessStartWhenRestoreRequired() {
        assertThatThrownBy(() -> NoJarTestJob.restoreCount(List.of(), false, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("restore is required");
    }

    @Test
    void rejectsMissingOrMalformedRestoredState() {
        assertThatThrownBy(() -> NoJarTestJob.restoreCount(List.of(), true, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing monotonic-counter-v1");
        assertThatThrownBy(() -> NoJarTestJob.restoreCount(List.of(-1L), true, false))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> NoJarTestJob.restoreCount(List.of(1L, 2L), true, false))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> NoJarTestJob.restoreCount(List.of(1L), false, false))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void refusesPermissiveRestoreConfiguration() {
        final Configuration configuration = new Configuration();
        NoJarTestJob.verifyRestoreSettings(configuration);
        configuration.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        assertThatThrownBy(() -> NoJarTestJob.verifyRestoreSettings(configuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ignore-unclaimed-state must be false");
    }

    @Test
    void rejectsBlobCacheCopyEvenWhenBytesMatch(@TempDir Path directory) throws Exception {
        final Path image = Files.writeString(directory.resolve("image.jar"), "identical");
        final Path blob = Files.writeString(directory.resolve("blob.jar"), "identical");
        ImageLocalProof.requireFile(image.toUri().toURL(), image.toString());
        assertThatThrownBy(
                        () -> ImageLocalProof.requireFile(blob.toUri().toURL(), image.toString()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected image-local");
    }
}
