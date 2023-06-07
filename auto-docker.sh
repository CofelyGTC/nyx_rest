#/bin/bash
filename="./sources/nyx_rest_api_plus.py"

sed -i '' "s#^VERSION=\".*\"#VERSION=\"$1\"#" $filename


docker build -t engiecofely/nyx_restapi_8:v$1 -f Dockerfile8-3.9 .
docker push engiecofely/nyx_restapi_8:v$1
