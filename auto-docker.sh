#/bin/bash
docker build -t engiecofely/nyx_restapi_8:v$1 -f Dockerfile8-3.9 .
docker push engiecofely/nyx_restapi_8:v$1
