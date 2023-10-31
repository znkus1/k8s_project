FROM spark:3.5.0-scala2.12-java11-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

RUN pip install Flask
RUN pip install numpy
RUN pip install pyspark
RUN pip install minio
RUN pip install gunicorn

ARG MINIO_SERVER_IP
ENV MINIO_SERVER_IP=$MINIO_SERVER_IP

ARG MINIO_PORT
ENV MINIO_PORT=$MINIO_PORT

ARG MINIO_ID
ENV MINIO_ID=$MINIO_ID

ARG MINIO_PASSWORD
ENV MINIO_PASSWORD=$MINIO_PASSWORD

COPY . .

WORKDIR ./myapp

CMD gunicorn main:app --bind 0.0.0.0:5000

EXPOSE 5000