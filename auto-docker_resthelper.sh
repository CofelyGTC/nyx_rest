#/bin/bash
filename="./sources/nyx_rest_helper.py"
VERSION="1.0.0"

sed -i "s/^VERSION=\".*\"/VERSION=\"$VERSION\"/" $filename #For Windows
sed -i '' "s/^VERSION=\".*\"/VERSION=\"$VERSION\"/" $filename #For MACOS


docker build -t engiecofely/nyx_rest_helper:v$VERSION -f Dockerfile .
docker push engiecofely/nyx_rest_helper:v$VERSION