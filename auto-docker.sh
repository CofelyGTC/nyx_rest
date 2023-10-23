#/bin/bash
filename="./sources/nyx_rest_api_plus.py"
VERSION="4.4.19"

sed -i "s/^VERSION=\".*\"/VERSION=\"$VERSION\"/" $filename #For Windows
sed -i '' "s/^VERSION=\".*\"/VERSION=\"$VERSION\"/" $filename #For MACOS


docker build -t engiecofely/nyx_restapi_8:v$VERSION -f Dockerfile .
docker push engiecofely/nyx_restapi_8:v$VERSION

### VERSION HISTORY ###

#   4.2.1   JFI Breaking Change Azure Signin
#   4.2.2   JFI One function for both azure and password
#   4.2.5   JFI Env redirect url
#   4.2.7   JFI Automatic UI version read
#   4.2.9   JFI Added Azure logout logic
#   4.4.2   JFI Password reset added
#   4.4.3   JFI Merged
#   4.4.10  JFI updated Postrges
#   4.4.15  EBU Optiboard Get Carousel


