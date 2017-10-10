#!/bin/bash

set -x


go get -u gopkg.in/yaml.v2

go get -u --insecure git.apache.org/thrift.git/lib/go/thrift


cd src/git.apache.org/thrift.git
git checkout tags/0.10.0
cd ../../..


set +x

