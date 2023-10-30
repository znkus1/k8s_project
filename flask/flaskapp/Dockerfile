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

ENV MINIO_SERVER_IP=[프라이빗 IP 입력]
ENV MINIO_PORT=[포트 입력]
ENV MINIO_ID=[아이디 입력]
ENV MINIO_PASSWORD=[비밀번호 입력]

COPY . .

WORKDIR ./myapp

CMD gunicorn main:app --bind 0.0.0.0:5000

EXPOSE 5000