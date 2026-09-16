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

"""Checks that the candidate operator still drives a Flink runtime that predates FLINK-33926.

The backported client relaxes a submission-time check, so an older runtime must keep working
unchanged, and a classpath-only job it cannot run must be refused before anything is deployed.
Reuses run.py so every kubectl call keeps the same local-cluster guard.
"""
import argparse
import json

from run import NAMESPACE, Demo

IMAGE = "flink:1.20.4-scala_2.12-java17"
JAR = "local:///opt/flink/examples/streaming/StateMachineExample.jar"
ENTRY_CLASS = "org.apache.flink.streaming.examples.statemachine.StateMachineExample"
NAME = "compat-120"


def manifest(jar):
    job = {"entryClass": ENTRY_CLASS, "args": ["--error-rate", "0.0", "--sleep", "10"],
           "parallelism": 1, "upgradeMode": "stateless", "state": "running"}
    if jar:
        job["jarURI"] = JAR
    return {"apiVersion": "flink.apache.org/v1beta1", "kind": "FlinkDeployment",
            "metadata": {"name": NAME, "namespace": NAMESPACE},
            "spec": {
                "image": IMAGE, "imagePullPolicy": "Never", "flinkVersion": "v1_20",
                "mode": "native", "serviceAccount": "flink",
                "flinkConfiguration": {
                    "taskmanager.numberOfTaskSlots": "1",
                    # 1.20 predates the canonical execution.checkpointing.* keys used elsewhere.
                    "state.checkpoints.dir": "file:///tmp/compat-checkpoints",
                    "state.savepoints.dir": "file:///tmp/compat-savepoints",
                    "execution.checkpointing.interval": "2 s"},
                "jobManager": {"resources": {"requests": {"memory": "1024m", "cpu": "0.5"}}},
                "taskManager": {"resources": {"requests": {"memory": "1024m", "cpu": "0.5"}}},
                "job": job}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    demo = Demo(argparse.Namespace(**vars(args), phase="compatibility"))

    # An ordinary JAR on a pre-FLINK-33926 runtime must be unaffected by the backport.
    demo.k("apply", "-f", "-", stdin=json.dumps(manifest(True)))
    demo.running(NAME)
    job_id = demo.job(NAME)["jid"]
    demo.save("compat-120-running.json", demo.get("flinkdeployment", NAME))
    print(f"PASS ordinary JAR on {IMAGE} reached RUNNING with checkpoints")

    # The same runtime cannot load a job from its image, so asking for that must be refused
    # by validation, leaving the running job untouched.
    spec = manifest(False)
    spec["spec"]["job"]["jarURI"] = None
    demo.k("apply", "-f", "-", stdin=json.dumps(spec))
    demo.wait("classpath-only rejected on unsupported runtime", lambda:
              "require Flink 2.1 or newer"
              in demo.get("flinkdeployment", NAME)["status"].get("error", ""))
    assert demo.job(NAME)["jid"] == job_id, "validation must not disturb the running job"
    assert demo.job(NAME)["state"] == "RUNNING", demo.job(NAME)
    demo.save("compat-120-rejected.json", demo.get("flinkdeployment", NAME))
    print("PASS classpath-only refused on 1.20 without disturbing the running job")

    demo.collect("compatibility")
    demo.k("delete", "flinkdeployment", NAME, "--wait=true", "--timeout=120s")


if __name__ == "__main__":
    main()
