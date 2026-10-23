# PySpark 4.0 + Delta Lake 4.0 + JupyterLab + pytest, em Linux.
# Spark no Windows exige winutils/HADOOP_HOME para gravar; aqui não.
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/work/src

COPY docker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Baixa os jars do Delta no build (ficam no ~/.ivy2 da imagem): rodar a fatia
# não depende de internet.
COPY docker/aquecer_delta.py /tmp/aquecer_delta.py
RUN python /tmp/aquecer_delta.py

WORKDIR /work
