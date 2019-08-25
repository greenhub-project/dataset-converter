#!/usr/bin/env sh

IMAGE=dataset-converter
docker save $(docker image ls -q $IMAGE:latest) | sudo docker-squash -t $IMAGE | docker load
