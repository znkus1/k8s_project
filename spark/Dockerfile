FROM openlake/spark-py:3.3.1
USER root
WORKDIR /app
RUN pip3 install pyspark==3.3.1
RUN pip3 install pyspark2pmml
COPY src/*.py .