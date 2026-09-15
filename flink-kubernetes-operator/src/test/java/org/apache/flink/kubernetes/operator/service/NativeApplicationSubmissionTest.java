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

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.KubernetesClusterClientFactory;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.artifact.DefaultKubernetesArtifactUploader;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ConfigMap;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Pod;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.utils.Serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the operator configuration and bundled native client, mocking only Kubernetes I/O. */
class NativeApplicationSubmissionTest {

    private static final String ENTRY_CLASS = "example.ImageApplication";
    private static final String[] ARGS = {"--message", "hello world", "--count", "3"};

    @TempDir private Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void applicationWithoutJarReachesKubernetes(boolean explicitNull) throws Exception {
        var deployment = deployment(null);
        var mapper = new ObjectMapper();
        ObjectNode json = mapper.valueToTree(deployment);
        var job = (ObjectNode) json.path("spec").path("job");
        if (explicitNull) {
            job.putNull("jarURI");
        } else {
            job.remove("jarURI");
        }
        deployment = mapper.treeToValue(json, FlinkDeployment.class);
        var config = buildConfig(deployment, new Configuration());
        assertThat(config.contains(PipelineOptions.JARS)).isFalse();

        var serialized = deploy(config, deployment);
        assertThat(config.contains(PipelineOptions.JARS)).isFalse();
        assertThat(serialized.contains(PipelineOptions.JARS)).isFalse();
        assertThat(serialized.get(ApplicationConfiguration.APPLICATION_MAIN_CLASS))
                .isEqualTo(ENTRY_CLASS);
        assertThat(serialized.get(ApplicationConfiguration.APPLICATION_ARGS)).containsExactly(ARGS);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "local:///opt/flink/lib/application.jar",
                "file:///opt/flink/lib/application.jar"
            })
    void ordinaryJarIsPreserved(String jar) throws Exception {
        var deployment = deployment(jar);
        var config = buildConfig(deployment, new Configuration());
        assertThat(deploy(config, deployment).get(PipelineOptions.JARS)).containsExactly(jar);
    }

    @Test
    void inheritedJarIsNotSilentlyRemoved() throws Exception {
        var deployment = deployment(null);
        var defaults = new Configuration();
        defaults.set(PipelineOptions.JARS, List.of("local:///opt/flink/lib/inherited.jar"));
        var config = buildConfig(deployment, defaults);
        assertThat(deploy(config, deployment).get(PipelineOptions.JARS))
                .containsExactly("local:///opt/flink/lib/inherited.jar");
    }

    @Test
    void explicitJarOverridesInheritedJars() throws Exception {
        var deployment = deployment("local:///opt/flink/lib/explicit.jar");
        var defaults = new Configuration();
        defaults.set(PipelineOptions.JARS, List.of("local:///first.jar", "local:///second.jar"));
        assertThat(deploy(buildConfig(deployment, defaults), deployment).get(PipelineOptions.JARS))
                .containsExactly("local:///opt/flink/lib/explicit.jar");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "[]"})
    void emptyOrBlankInheritedJarsAreRejected(String jars) throws Exception {
        var deployment = deployment(null);
        var defaults = new Configuration();
        defaults.setString(PipelineOptions.JARS.key(), jars);
        var config = buildConfig(deployment, defaults);
        assertThatThrownBy(() -> deploy(config, deployment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one jar");
    }

    @Test
    void emptyInheritedJarListIsRejected() throws Exception {
        var deployment = deployment(null);
        var defaults = new Configuration();
        defaults.set(PipelineOptions.JARS, List.of());
        var config = buildConfig(deployment, defaults);
        assertThatThrownBy(() -> deploy(config, deployment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one jar");
    }

    @Test
    void multipleInheritedJarsAreRejected() throws Exception {
        var deployment = deployment(null);
        var defaults = new Configuration();
        defaults.set(PipelineOptions.JARS, List.of("local:///first.jar", "local:///second.jar"));
        var config = buildConfig(deployment, defaults);
        assertThatThrownBy(() -> deploy(config, deployment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one jar");
    }

    @Test
    void enabledUploaderCopiesOrdinaryLocalJar() throws Exception {
        var source = tempDir.resolve("application.jar");
        Files.writeString(source, "test artifact bytes");
        var target = Files.createDirectory(tempDir.resolve("uploads"));
        var deployment = deployment("local://" + source);
        var defaults = new Configuration();
        defaults.set(KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED, true);
        defaults.set(KubernetesConfigOptions.LOCAL_UPLOAD_TARGET, target.toUri().toString());
        var config = buildConfig(deployment, defaults);
        var submitted = deploy(config, deployment);
        assertThat(submitted.get(PipelineOptions.JARS)).hasSize(1);
        assertThat(
                        new org.apache.flink.core.fs.Path(
                                        submitted.get(PipelineOptions.JARS).get(0))
                                .toUri()
                                .getPath())
                .isEqualTo(target.resolve("application.jar").toString());
        assertThat(target.resolve("application.jar")).hasSameBinaryContentAs(source);
    }

    private FlinkDeployment deployment(String jar) {
        var deployment = TestUtils.buildApplicationCluster(FlinkVersion.v2_3);
        deployment.getSpec().getJob().setJarURI(jar);
        deployment.getSpec().getJob().setEntryClass(ENTRY_CLASS);
        deployment.getSpec().getJob().setArgs(ARGS);
        // Avoid unrelated resource-generated pod templates; keep all submission logic real.
        deployment.getSpec().getJobManager().setResources(null);
        deployment.getSpec().getJobManager().setResource(new Resource(1.0, "1024m", null));
        deployment.getSpec().getTaskManager().setResources(null);
        deployment.getSpec().getTaskManager().setResource(new Resource(1.0, "1024m", null));
        return deployment;
    }

    private Configuration buildConfig(FlinkDeployment deployment, Configuration defaults)
            throws Exception {
        defaults.set(DeploymentOptionsInternal.CONF_DIR, tempDir.toString());
        return FlinkConfigBuilder.buildFrom(
                deployment.getMetadata().getNamespace(),
                deployment.getMetadata().getName(),
                deployment.getSpec(),
                defaults);
    }

    private Configuration deploy(Configuration config, FlinkDeployment deployment)
            throws Exception {
        var resources = new AtomicReference<KubernetesJobManagerSpecification>();
        var client =
                (FlinkKubeClient)
                        Proxy.newProxyInstance(
                                FlinkKubeClient.class.getClassLoader(),
                                new Class<?>[] {FlinkKubeClient.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "getService":
                                            return Optional.empty();
                                        case "getRestEndpoint":
                                            return Optional.of(new Endpoint("localhost", 8081));
                                        case "createJobManagerComponent":
                                            assertThat(resources.get()).isNull();
                                            resources.set(
                                                    (KubernetesJobManagerSpecification) args[0]);
                                            return null;
                                        case "loadPodFromTemplateFile":
                                            try (var input =
                                                    Files.newInputStream(
                                                            ((File) args[0]).toPath())) {
                                                return new KubernetesPod(
                                                        Serialization.unmarshal(input, Pod.class));
                                            }
                                        case "close":
                                            return null;
                                        default:
                                            throw new AssertionError(
                                                    "Unexpected Kubernetes call: " + method);
                                    }
                                });
        var factory =
                new FlinkKubeClientFactory() {
                    @Override
                    public FlinkKubeClient fromConfiguration(
                            Configuration ignored, String useCase) {
                        return client;
                    }
                };
        try (var descriptor =
                new KubernetesClusterDescriptor(
                        config, factory, new DefaultKubernetesArtifactUploader())) {
            descriptor.deployApplicationCluster(
                    new KubernetesClusterClientFactory().getClusterSpecification(config),
                    new ApplicationConfiguration(
                            deployment.getSpec().getJob().getArgs(),
                            deployment.getSpec().getJob().getEntryClass()));
        }
        assertThat(resources.get()).isNotNull();
        var configMap =
                resources.get().getAccompanyingResources().stream()
                        .filter(ConfigMap.class::isInstance)
                        .map(ConfigMap.class::cast)
                        .filter(
                                cm ->
                                        cm.getData().containsKey("config.yaml")
                                                || cm.getData().containsKey("flink-conf.yaml"))
                        .findFirst()
                        .orElseThrow();
        var serializedDirectory = Files.createTempDirectory(tempDir, "serialized-");
        for (var file : configMap.getData().entrySet()) {
            Files.writeString(serializedDirectory.resolve(file.getKey()), file.getValue());
        }
        return GlobalConfiguration.loadConfiguration(serializedDirectory.toString());
    }
}
