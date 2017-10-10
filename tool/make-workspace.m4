#!/bin/bash
dnl # m4 -D PACKAGE_PREFIX=github.com/yinyin -D PACKAGE_NAME=watchback make-workspace.m4 > make-workspace.sh

# Usage: REPO_URL=... BRANCH=... WORKSPACE=... bash make-workspace.sh

if [ -z "${REPO_URL}" ]; then
	REPO_URL="https://PACKAGE_PREFIX/PACKAGE_NAME"
fi
if [ -z "${BRANCH}" ]; then
	BRANCH=""
fi
if [ -z "${WORKSPACE}" ]; then
	WORKSPACE="PACKAGE_NAME-workspace"
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

mkdir -p "${WORKSPACE}/src/PACKAGE_PREFIX"
cd "${WORKSPACE}/src/PACKAGE_PREFIX/"
git clone "${REPO_URL}" PACKAGE_NAME
cd "PACKAGE_NAME/"
if [ ! -z "${BRANCH}" ]; then
	git checkout "${BRANCH}"
fi
cd "${BASE_WORK_PATH}/${WORKSPACE}/"

ln -s "src/PACKAGE_PREFIX/PACKAGE_NAME/tool" tool
ln -s "src/PACKAGE_PREFIX/PACKAGE_NAME/tool/env-setup.sh" .env.sh

