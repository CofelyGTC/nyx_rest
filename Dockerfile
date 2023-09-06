FROM python:3.9.13-slim
MAINTAINER snuids

RUN apt-get update
RUN apt-get install -y vim

COPY ./sources/requirements.txt /opt/sources/requirements.txt
RUN pip install -r /opt/sources/requirements.txt 

COPY ./sources /opt/sources
RUN mkdir  /opt/sources/logs

WORKDIR /opt/sources

CMD ["python", "nyx_rest_api_plus.py"]
