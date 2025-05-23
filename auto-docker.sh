#/bin/bash
filename="./sources/nyx_rest_api_plus.py"
VERSION="4.4.39"

#sed -i "s/^VERSION=\".*\"/VERSION=\"$VERSION\"/" $filename #For Windows
#sed -i '' "s/^VERSION=\".*\"/VERSION=\"$VERSION\"/" $filename #For MACOS


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
#   4.4.21  EBU Add Title as env variable
#   4.4.22  EBU Security Update && Debug load data multiple pages
#   4.4.23  JFI Added SKIP_ACTIVE_DIRECTORY
#   4.4.25  EBU Transfer counteruser from quantesrestapi
#   4.4.26  JFI correcting report filter for users
#   4.4.28  EBU Optiboard: Redis.set Change json for use generic search
#   4.4.29  EBU Optiboard: Condition if not timestamp
#   4.4.30  EBU Optiboard: Function send usage log when mode 4G
#   4.4.31  EBU Optiboard: Fix timestamp - isoformat
#   4.4.34  EBU Optiboard: New meteo logo
#   4.4.35  EBU Optiboard: Fix meteo URL
#   4.4.36  EBU Optiboard: Add getweather function
#   4.4.37  EBU Optiboard: Add token kibananyx
#   4.4.38  EBU AMQSTOMPCLIENT: Use local script