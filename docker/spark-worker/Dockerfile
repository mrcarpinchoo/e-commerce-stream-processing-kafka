FROM spark-base:3.5

EXPOSE 8081

CMD ["/opt/spark/sbin/start-worker.sh", "spark://spark-master:7077", "--webui-port", "8081"]
