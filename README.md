Demonstration of streaming data pipeline utilizing

Dataset : https://www.kaggle.com/datasets/kzmontage/e-commerce-website-logs

Architecture : 

    services:
    namenode:
        image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

    datanode:
        image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8

    spark-master:
        image: bitnami/spark:3.5

    spark-worker:
        image: bitnami/spark:3.5

    spark-producer:
        image: bitnami/spark:3.5

    spark-consumer:
        image: bitnami/spark:3.5

    kafka:
        image: bitnami/kafka:3.7

    kafka-init:
        image: bitnami/kafka:3.7

    kafka-ui:
        image: provectuslabs/kafka-ui:latest

    cassandra:
        image: cassandra:latest

    cassandra-init:
        image: cassandra:latest

    grafana:
        image: grafana/grafana:latest