#!/usr/bin/env bash

PROJECT_VERSION=$(mvn -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec -q)

docker push aone.docker.com:80/polaris:${PROJECT_VERSION}