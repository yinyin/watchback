#!/bin/bash

# Usage: REPO_URL=... BRANCH=... WORKSPACE=... bash make-workspace.sh

if [ -z "${REPO_URL}" ]; then
	REPO_URL="https://github.com/yinyin/watchback"
fi
if [ -z "${BRANCH}" ]; then
	BRANCH=""
fi
if [ -z "${WORKSPACE}" ]; then
	WORKSPACE="watchback-workspace"
fi

echo "REPO_URL=${REPO_URL}"
echo "BRANCH=${BRANCH}"
echo "WORKSPACE=${WORKSPACE}"

if [ -d "${WORKSPACE}" ]; then
	echo "ERR: ${WORKSPACE}/ already existed."
	exit
fi

BASE_WORK_PATH="${PWD}"
if [ -z "${BASE_WORK_PATH}" ]; then
	echo "ERR: expect environment PWD."
	exit
fi

mkdir -p "${WORKSPACE}/src/github.com/yinyin"
cd "${WORKSPACE}/src/github.com/yinyin/"
git clone "${REPO_URL}" watchback
cd "watchback/"
if [ ! -z "${BRANCH}" ]; then
	git checkout "${BRANCH}"
fi
cd "${BASE_WORK_PATH}/${WORKSPACE}/"

ln -s "src/github.com/yinyin/watchback/tool" tool
ln -s "src/github.com/yinyin/watchback/tool/env-setup.sh" .env.sh

