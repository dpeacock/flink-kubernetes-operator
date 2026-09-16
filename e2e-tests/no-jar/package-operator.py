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

"""Build a local-only operator image from a completed Maven reactor build."""
import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--variant", choices=["baseline", "candidate"], required=True)
    args = parser.parse_args()
    source = Path(args.source).resolve()
    with tempfile.TemporaryDirectory(prefix="flink-40677-image-") as tmp:
        target = Path(tmp)
        for pattern in ["flink-kubernetes-operator/target/*-shaded.jar",
                        "flink-kubernetes-webhook/target/*-shaded.jar",
                        "flink-kubernetes-standalone/target/flink-kubernetes-standalone-1.16.0.jar",
                        "LICENSE", "NOTICE", "docker-entrypoint.sh"]:
            files = list(source.glob(pattern))
            if len(files) != 1:
                raise RuntimeError(f"Expected exactly one built file for {pattern}: {files}")
            file = files[0]
            destination = target / file.relative_to(source)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file, destination)
        for name in ["plugins", "log4j", "logback"]:
            path = Path("flink-kubernetes-operator/target") / name
            shutil.copytree(source / path, target / path)
        shutil.copy2(Path(__file__).with_name("Dockerfile.operator"), target / "Dockerfile")
        subprocess.run(["docker", "build", "-t", f"flink-40677-operator:{args.variant}", str(target)], check=True)


if __name__ == "__main__":
    main()
