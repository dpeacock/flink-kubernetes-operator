/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.validation;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Classpath-only capability checks must also run without an admission webhook. */
class NativeApplicationValidationTest {

    @ParameterizedTest
    @EnumSource(
            value = FlinkVersion.class,
            names = {"v1_20", "v2_0"})
    void rejectsUnsupportedRuntime(FlinkVersion version) {
        var deployment = deployment();
        deployment.getSpec().setFlinkVersion(version);
        assertThat(validator(new Configuration()).validateDeployment(deployment))
                .contains("Native classpath-only applications require Flink 2.1 or newer.");
    }

    @ParameterizedTest
    @EnumSource(
            value = FlinkVersion.class,
            names = {"v2_1", "v2_2", "v2_3"})
    void acceptsCompatibleRuntime(FlinkVersion version) {
        var deployment = deployment();
        deployment.getSpec().setFlinkVersion(version);
        assertThat(validator(new Configuration()).validateDeployment(deployment)).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "   ")
    void requiresEntryClass(String entryClass) {
        var deployment = deployment();
        deployment.getSpec().getJob().setEntryClass(entryClass);
        assertThat(validator(new Configuration()).validateDeployment(deployment))
                .contains(
                        "job.entryClass must be defined for a native classpath-only application.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void rejectsBlankJobJar(String jar) {
        var deployment = deployment();
        deployment.getSpec().getJob().setJarURI(jar);
        assertThat(validator(new Configuration()).validateDeployment(deployment))
                .contains(
                        "job.jarURI must not be blank; omit it for a classpath-only application.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "[]", "local:///first.jar;local:///second.jar"})
    void rejectsMalformedInheritedJars(String jars) {
        var defaults = new Configuration();
        defaults.setString(PipelineOptions.JARS.key(), jars);
        assertThat(validator(defaults).validateDeployment(deployment()))
                .contains(
                        "pipeline.jars must contain exactly one JAR when configured; omit it for a classpath-only application.");
    }

    @Test
    void respectsEffectiveConfigurationPrecedence() {
        var defaults = new Configuration();
        defaults.set(PipelineOptions.JARS, List.of("local:///inherited.jar"));
        defaults.set(KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED, true);
        var deployment = deployment();
        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_20);
        deployment.getSpec().getJob().setEntryClass(null);
        assertThat(validator(defaults).validateDeployment(deployment)).isEmpty();

        defaults.set(PipelineOptions.JARS, List.of("local:///first.jar", "local:///second.jar"));
        deployment.getSpec().getJob().setJarURI("local:///explicit.jar");
        assertThat(validator(defaults).validateDeployment(deployment)).isEmpty();
    }

    @Test
    void rejectsEnabledUploaderBeforeSubmissionIncludingInheritedSetting() {
        var defaults = new Configuration();
        defaults.set(KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED, true);
        var deployment = deployment();
        assertThat(validator(defaults).validateDeployment(deployment))
                .contains(
                        "kubernetes.artifacts.local-upload-enabled must be false for a native classpath-only application.");
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED.key(), "false");
        assertThat(validator(defaults).validateDeployment(deployment)).isEmpty();
    }

    @Test
    void leavesStandaloneSessionAndPythonBehaviorUnchanged() {
        var deployment = deployment();
        deployment.getSpec().setFlinkVersion(FlinkVersion.v1_20);
        deployment.getSpec().getJob().setEntryClass(null);
        deployment.getSpec().setMode(KubernetesDeploymentMode.STANDALONE);
        var validator = validator(new Configuration());
        assertThat(validator.validateDeployment(deployment)).isEmpty();
        deployment.getSpec().setMode(KubernetesDeploymentMode.NATIVE);
        deployment.getSpec().getJob().setEntryClass("org.apache.flink.client.python.PythonDriver");
        assertThat(validator.validateDeployment(deployment)).isEmpty();
        deployment.getSpec().setJob(null);
        assertThat(validator.validateDeployment(deployment)).isEmpty();
    }

    private DefaultValidator validator(Configuration defaults) {
        return new DefaultValidator(new FlinkConfigManager(defaults));
    }

    private FlinkDeployment deployment() {
        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v2_3);
        deployment.getSpec().getJob().setJarURI(null);
        deployment.getSpec().getJob().setEntryClass("example.ImageApplication");
        return deployment;
    }
}
