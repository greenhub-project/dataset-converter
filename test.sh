#!/usr/bin/env sh

docker-compose down
docker rmi dataset-converter:latest
docker-compose up -d
