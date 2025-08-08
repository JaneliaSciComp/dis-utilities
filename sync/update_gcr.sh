#!/bin/bash
REPO=dis-sync
VERSION=11.7.0
IMAGE=`echo $REPO:$VERSION`
LATEST=`echo $REPO:latest`
echo $IMAGE
docker image build . --no-cache -t gcr.io/sandbox-220614/scsw/jenkins/$IMAGE --platform linux/amd64
docker image ls | grep $REPO
docker image ls | grep $REPO | grep $VERSION | awk '{print $3}' | xargs -J % -t docker image tag % gcr.io/sandbox-220614/scsw/jenkins/$IMAGE
docker image ls | grep $REPO | grep $VERSION | awk '{print $3}' | xargs -J % -t docker image tag % gcr.io/sandbox-220614/scsw/jenkins/$LATEST
gcloud auth login
gcloud auth configure-docker
CLOUDSDK_CORE_PROJECT=sandbox-220614
gcloud config set project sandbox-220614
docker push gcr.io/sandbox-220614/scsw/jenkins/$IMAGE
docker push gcr.io/sandbox-220614/scsw/jenkins/$LATEST
