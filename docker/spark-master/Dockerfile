FROM spark-base:4.0
LABEL author="Pablo Camarillo" email="pablo.camarillo@iteso.mx"
LABEL version="0.4"

RUN apt-get update && apt-get install -y dos2unix
COPY entrypoint.sh /usr/local/bin/
RUN dos2unix /usr/local/bin/entrypoint.sh && apt-get --purge remove -y dos2unix && rm -rf /var/lib/apt/lists/*

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_MASTER_LOG /spark/logs

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/usr/local/bin/entrypoint.sh"]
