# Utilisez une image de base qui prend en charge Java
FROM openjdk:8-jre-alpine

COPY ./target/scala-spark-1.2-jar-with-dependencies.jar /app/scala-spark.jar

RUN mkdir /data

ENV MASTER_URL="a"
ENV SRC_PATH="a"
ENV DST_PATH="a"
ENV GROUP_VAR="a"
ENV OP_VAR="a"
ENV Hive="a"
ENV app_prop="a"

ENTRYPOINT java -cp /app/scala-spark.jar fr.mosef.scala.template.Main ${MASTER_URL} ${SRC_PATH} ${DST_PATH} ${GROUP_VAR} ${OP_VAR} ${Hive} ${app_prop}

docker run --rm \
-e MASTER_URL='local[1]' \
-e SRC_PATH='/data/rappelconso0.csv' \
-e DST_PATH='/data/output/' \
-e GROUP_VAR='group_key' \
-e OP_VAR='ndeg_de_version' \
-e Hive='false' \
-e app_prop='/data/application.properties'
-v /Users/axel/Documents/École/Université/M2/M2_S2/Scala/TP/scala-projet-axelfritz2/src/main/resources:/data \
-v /Users/axel/Documents/École/Université/M2/M2_S2/Scala/TP/scala-projet-axelfritz2/default:/data/output \
scala-spark-image

