FROM python:3.8.2-buster

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

COPY requirements.txt requirements.txt

COPY . project

RUN pip install -r requirements.txt
