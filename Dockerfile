FROM python:3.9.13-slim
ARG VERSION

ENV VERSION=${VERSION}}

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y vim

COPY ./sources/requirements.txt /opt/sources/requirements.txt
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r /opt/sources/requirements.txt 

COPY ./sources /opt/sources
RUN mkdir  /opt/sources/logs

WORKDIR /opt/sources

CMD ["python", "nyx_rest_api_plus.py"]
