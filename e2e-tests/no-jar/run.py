#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Disposable kind-only FLINK-40677 proof. Never uses the default kubeconfig."""
import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
CONTEXT = "kind-flink-40677"
NAMESPACE = "flink-40677"
APP_IMAGE = "flink-40677-app:fixture"
OPERATOR = "flink-40677-operator"


def command(args, *, stdin=None, check=True):
    result = subprocess.run(args, input=stdin, text=True, capture_output=True, timeout=180)
    if check and result.returncode:
        raise RuntimeError(f"{args}: {result.stdout}\n{result.stderr}")
    return result


class Demo:
    def __init__(self, args):
        self.args = args
        self.out = Path(args.evidence).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.kbase = ["kubectl", "--kubeconfig", str(Path(args.kubeconfig).resolve()),
                      "--context", CONTEXT, "--namespace", NAMESPACE, "--request-timeout=30s"]
        self.phase = args.phase

    def guard(self):
        config = json.loads(command(self.kbase + ["config", "view", "--minify", "-o", "json"]).stdout)
        server = urlparse(config["clusters"][0]["cluster"]["server"])
        assert server.scheme == "https" and server.hostname in ("127.0.0.1", "::1"), server
        assert config["contexts"][0]["name"] == CONTEXT, config

    def k(self, *args, stdin=None, check=True):
        # Includes exec, delete, patch and apply: recheck before EVERY command.
        self.guard()
        return command(self.kbase + list(args), stdin=stdin, check=check).stdout

    def save(self, name, content):
        if not isinstance(content, str):
            content = json.dumps(content, indent=2)
        (self.out / f"{self.phase}-{name}").write_text(content)

    def wait(self, description, predicate, seconds=300):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    print(f"PASS {description}", flush=True)
                    return value
            except (RuntimeError, KeyError, IndexError, json.JSONDecodeError) as error:
                last = str(error)
            time.sleep(2)
        self.collect("timeout")
        raise AssertionError(f"Timeout: {description}; last error: {last}")

    def get(self, kind, name):
        return json.loads(self.k("get", kind, name, "-o", "json"))

    def pods(self, name, component):
        data = json.loads(self.k("get", "pods", "-l", f"app={name},component={component}", "-o", "json"))
        return [p for p in data["items"] if not p["metadata"].get("deletionTimestamp")
                and p["status"].get("phase") == "Running"]

    def pod(self, name, component):
        return self.pods(name, component)[0]["metadata"]["name"]

    def logs(self, pod):
        return self.k("logs", pod, "--all-containers=true", "--tail=10000")

    def rest(self, name, path):
        return json.loads(self.k("exec", self.pod(name, "jobmanager"), "-c", "flink-main-container",
                                "--", "curl", "--fail", "--silent", "http://localhost:8081" + path))

    def job(self, name):
        return self.rest(name, "/jobs/overview")["jobs"][0]

    def checkpoints(self, name):
        return self.rest(name, f"/jobs/{self.job(name)['jid']}/checkpoints")

    def running(self, name):
        self.wait(f"{name} RUNNING with completed checkpoints", lambda:
                  self.job(name)["state"] == "RUNNING"
                  and self.checkpoints(name)["counts"]["completed"] >= 2)

    def install_operator(self, version):
        values = {"image": {"repository": OPERATOR, "tag": version, "pullPolicy": "Never"},
                  "watchNamespaces": [NAMESPACE], "webhook": {"create": False},
                  "defaultConfiguration": {"flink-conf.yaml":
                      "kubernetes.operator.reconcile.interval: 2 s\n"
                      "kubernetes.operator.observer.progress-check.interval: 2 s\n"
                      "kubernetes.operator.snapshot.resource.enabled: false\n"
                      "kubernetes.operator.exception.stacktrace.enabled: true\n"
                      "kubernetes.operator.exception.stacktrace.max.length: 20000\n"}}
        path = self.out / f"operator-{version}-values.json"
        path.write_text(json.dumps(values))
        rendered = command(["helm", "template", "operator", str(ROOT / "helm/flink-kubernetes-operator"),
                            "--namespace", NAMESPACE, "--include-crds", "-f", str(path)]).stdout
        self.save("operator.yaml", rendered)
        self.k("apply", "-f", "-", stdin=rendered)
        self.k("rollout", "status", "deployment/flink-kubernetes-operator", "--timeout=180s")
        self.save("operator-image.json", self.get("deployment", "flink-kubernetes-operator"))

    def manifest(self, name, jar):
        spec = {
            "image": APP_IMAGE, "imagePullPolicy": "Never", "flinkVersion": "v2_3", "mode": "native",
            "serviceAccount": "flink",
            "flinkConfiguration": {
                "taskmanager.numberOfTaskSlots": "1",
                "kubernetes.artifacts.local-upload-enabled": "false",
                "high-availability.type": "kubernetes", "high-availability.storageDir": "file:///state/ha",
                "execution.checkpointing.storage": "filesystem",
                "execution.checkpointing.dir": "file:///state/checkpoints",
                "execution.checkpointing.savepoint-dir": "file:///state/savepoints",
                "execution.checkpointing.interval": "2 s",
                "execution.checkpointing.externalized-checkpoint-retention": "RETAIN_ON_CANCELLATION",
                "execution.state-recovery.ignore-unclaimed-state": "false",
                "kubernetes.operator.snapshot.resource.enabled": "false",
                "heartbeat.interval": "1000", "heartbeat.timeout": "10000",
                "restart-strategy.type": "fixed-delay", "restart-strategy.fixed-delay.delay": "1 s",
                "blob.storage.directory": "/tmp/flink-blobs",
                "process.working-dir": "/tmp/flink-working"},
            "podTemplate": {"spec": {
                "initContainers": [{"name": "state-permissions", "image": APP_IMAGE,
                    "imagePullPolicy": "Never", "securityContext": {"runAsUser": 0},
                    "command": ["sh", "-c", "chmod 777 /state"],
                    "volumeMounts": [{"name": "state", "mountPath": "/state"}]}],
                "containers": [{"name": "flink-main-container",
                    "volumeMounts": [{"name": "state", "mountPath": "/state"}]}],
                "volumes": [{"name": "state", "hostPath": {
                    "path": f"/var/local/flink-40677/{name}", "type": "DirectoryOrCreate"}}]}},
            "jobManager": {"resources": {"requests": {"memory": "1024m", "cpu": "0.5"}}},
            "taskManager": {"resources": {"requests": {"memory": "1024m", "cpu": "0.5"}}},
            "job": {"entryClass": "org.apache.flink.kubernetes.operator.e2e.nojar.NoJarTestJob",
                    "args": ["--interval-ms", "100", "--checkpoint-interval-ms", "1000"], "parallelism": 1, "upgradeMode": "savepoint",
                    "state": "running", "allowNonRestoredState": False}}
        if jar:
            spec["job"]["jarURI"] = "local:///opt/flink/lib/no-jar-test.jar"
        return {"apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkDeployment",
                "metadata": {"name": name, "namespace": NAMESPACE}, "spec": spec}

    def apply_job(self, name, jar):
        manifest = self.manifest(name, jar)
        encoded = json.dumps(manifest, sort_keys=True, indent=2)
        fixture = self.out / f"{name}-fixture.json"
        if fixture.exists():
            assert fixture.read_text() == encoded, "baseline/candidate fixture changed"
        else:
            fixture.write_text(encoded)
        self.k("apply", "-f", "-", stdin=encoded)
        actual = self.get("flinkdeployment", name)
        assert actual["spec"]["image"] == APP_IMAGE
        if not jar:
            assert "jarURI" not in actual["spec"]["job"]
            assert "pipeline.jars" not in actual["spec"]["flinkConfiguration"]
        self.save(f"{name}-submitted.json", actual)

    def collect(self, suffix):
        self.save(f"{suffix}-resources.json", self.k("get", "pods,flinkdeployments,configmaps", "-o", "json"))
        pods = json.loads(self.k("get", "pods", "-o", "json"))["items"]
        for p in pods:
            name = p["metadata"]["name"]
            self.save(f"{suffix}-{name}.log", self.k("logs", name, "--all-containers=true", check=False))
        self.save(f"{suffix}-events.txt", self.k("get", "events", "--sort-by=.lastTimestamp"))

    def proof(self, name, jar_count, label, restored=False):
        self.running(name)
        jm = self.pod(name, "jobmanager")
        tm = self.pod(name, "taskmanager")
        jm_logs, tm_logs = self.logs(jm), self.logs(tm)
        assert "IMAGE_LOCAL role=jobmanager" in jm_logs
        assert "IMAGE_LOCAL role=taskmanager" in tm_logs
        results = re.findall(r"RESULT n=(\d+) value=(\d+) restored=(true|false) attempt=(\d+)", tm_logs)
        assert len(results) >= 5, results
        previous = {}
        for n, value, _, attempt in results:
            n, value = int(n), int(value)
            assert value == 17 * n + 7, (n, value)
            if attempt in previous:
                assert n == previous[attempt] + 1, (previous[attempt], n)
            previous[attempt] = n
        if restored:
            states = re.findall(r"STATE_INIT restored=true n=(\d+) checkpoint=(\d+)", tm_logs)
            assert states and int(states[-1][0]) > 0, states
            assert any(r == "true" for _, _, r, _ in results)
        conf = {c["key"]: c["value"] for c in self.rest(name, "/jobmanager/config")}
        assert conf["execution.state-recovery.ignore-unclaimed-state"] == "false", conf
        if jar_count == 0:
            assert "pipeline.jars" not in conf, conf
        else:
            assert "no-jar-test.jar" in conf["pipeline.jars"], conf
        self.save(f"{label}-effective-config.json", conf)
        self.save(f"{label}-checkpoints.json", self.checkpoints(name))
        paths = self.k("exec", jm, "-c", "flink-main-container", "--", "find", "/state/ha",
                       "-name", "submittedExecutionPlan*", "-type", "f").splitlines()
        assert len(paths) == 1, paths
        plan = self.k("exec", jm, "-c", "flink-main-container", "--", "java", "-cp", "/opt/flink/lib/*",
                      "org.apache.flink.kubernetes.operator.e2e.nojar.InspectExecutionPlan", paths[0], str(jar_count))
        assert self.job(name)["jid"] in plan, plan
        self.save(f"{label}-persisted-graph.txt", plan)
        for role, pod in [("jm", jm), ("tm", tm)]:
            data = self.get("pod", pod)
            image_id = next(c["imageID"] for c in data["status"]["containerStatuses"]
                            if c["name"] == "flink-main-container")
            identity = self.out / "application-image-id.txt"
            if identity.exists():
                assert identity.read_text() == image_id, (identity.read_text(), image_id)
            else:
                identity.write_text(image_id)
            self.save(f"{label}-{role}-pod.json", data)
            evidence = self.k("exec", pod, "-c", "flink-main-container", "--", "sh", "-c",
                "sha256sum /opt/flink/lib/no-jar-test*.jar; "
                "find /tmp/flink-blobs /tmp/flink-working /state/ha -type f -name 'blob_*' -exec sha256sum {} \\;")
            app_hash = next(line.split()[0] for line in evidence.splitlines()
                            if line.endswith("/opt/flink/lib/no-jar-test.jar"))
            blob_lines = [line for line in evidence.splitlines() if "/blob_" in line]
            if jar_count:
                assert any(line.startswith(app_hash) for line in blob_lines), evidence
            else:
                assert not blob_lines, evidence
            self.save(f"{label}-{role}-blobs.txt", evidence)
            self.save(f"{label}-{role}.log", self.logs(pod))
        print(f"PASS {label}: deterministic worker output, image identity, strict state, persisted graph JARs={jar_count}, BLOB/cache evidence", flush=True)

    def baseline(self):
        self.k("create", "namespace", NAMESPACE)
        self.install_operator("baseline")
        self.apply_job("nojar-test", False)
        self.wait("baseline absent-JAR native descriptor failure", lambda:
                  "NullPointerException" in self.get("flinkdeployment", "nojar-test")["status"].get("error", ""))
        error = self.get("flinkdeployment", "nojar-test")["status"]["error"]
        assert "KubernetesUtils.checkJarFileForApplicationMode" in error, error
        self.save("expected-error.json", json.loads(error))
        assert not json.loads(self.k("get", "deployments", "-l", "app=nojar-test", "-o", "json"))["items"]
        self.apply_job("jar-control", True)
        self.running("jar-control")
        self.proof("jar-control", 1, "ordinary-control")
        self.collect("baseline-red-control-green")
        self.k("delete", "flinkdeployment", "nojar-test", "--wait=true", "--timeout=60s")

    def candidate(self):
        self.install_operator("candidate")
        self.apply_job("nojar-test", False)
        self.running("nojar-test")
        # Force a fresh ordinary-JAR submission by the candidate, not just observation
        # of a job that the baseline operator started.
        old = self.pod("jar-control", "jobmanager")
        self.k("patch", "flinkdeployment", "jar-control", "--type=merge", "-p",
               json.dumps({"spec": {"restartNonce": 1}}))
        self.wait("candidate fresh ordinary-JAR JobManager", lambda:
                  self.pod("jar-control", "jobmanager") != old)
        self.running("jar-control")
        self.proof("jar-control", 1, "ordinary-control", restored=True)
        self.proof("nojar-test", 0, "nojar")
        self.collect("candidate-green")

    def recovery(self):
        name = "nojar-test"
        self.running(name)
        old_tm = self.pod(name, "taskmanager")
        old_uid = self.get("pod", old_tm)["metadata"]["uid"]
        before = self.checkpoints(name)
        self.save("before-worker-replacement-checkpoints.json", before)
        self.k("delete", "pod", old_tm, "--wait=true", "--timeout=60s")
        self.wait("fresh worker pod with fresh emptyDir/cache", lambda:
                  self.get("pod", self.pod(name, "taskmanager"))["metadata"]["uid"] != old_uid)
        self.running(name)
        self.wait("fresh worker restored checkpoint", lambda:
                  "STATE_INIT restored=true" in self.logs(self.pod(name, "taskmanager")))
        self.proof(name, 0, "worker-recovery", restored=True)

        old_jm = self.pod(name, "jobmanager")
        job_id = self.job(name)["jid"]
        completed_id = self.checkpoints(name)["latest"]["completed"]["id"]
        self.k("delete", "pod", old_jm, "--wait=true", "--timeout=60s")
        self.wait("replacement JobManager", lambda: self.pod(name, "jobmanager") != old_jm)
        self.running(name)
        assert self.job(name)["jid"] == job_id, "HA recovery must retain the job ID"
        self.wait("JobManager HA checkpoint restore", lambda:
                  self.checkpoints(name)["latest"]["restored"]["id"] >= completed_id)
        self.proof(name, 0, "jobmanager-ha-recovery", restored=True)

        name = "jar-control"
        for jar_count, label in [(0, "jar-to-nojar"), (1, "rollback-to-jar")]:
            old_jm = self.pod(name, "jobmanager")
            self.k("patch", "flinkdeployment", name, "--type=merge", "-p", json.dumps({"spec": {"job": {
                "jarURI": "local:///opt/flink/lib/no-jar-test.jar" if jar_count else None,
                "args": ["--interval-ms", "100", "--checkpoint-interval-ms", "1000", "--require-restored", "true"],
                "allowNonRestoredState": False, "upgradeMode": "savepoint"}}}))
            self.wait(label + " new JobManager", lambda: self.pod(name, "jobmanager") != old_jm)
            self.running(name)
            self.proof(name, jar_count, label, restored=True)
            checkpoints = self.checkpoints(name)
            assert checkpoints["latest"]["restored"]["is_savepoint"], checkpoints
            self.save(label + "-deployment.json", self.get("flinkdeployment", name))
        self.collect("recovery-complete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--phase", choices=["baseline", "candidate", "recovery"], required=True)
    args = parser.parse_args()
    demo = Demo(args)
    demo.guard()
    getattr(demo, args.phase)()


if __name__ == "__main__":
    main()
