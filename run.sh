#!/bin/bash

spark-submit \
    --master local \
    --deploy-mode client \
    --queue default \
    --driver-memory 512M \
    --executor-memory 512M \
    --num-executors 4 \
    --class SocialPageRank \
/home/sasaki/dev/IBM-Spark/social/target/scala-2.10/pagerank_2.10-0.1-SNAPSHOT.jar