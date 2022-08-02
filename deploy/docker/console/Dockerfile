###############################################################################
#  Copyright (c) 2019 The StreamX Project
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#  https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################
FROM alpine:3.16 as deps-stage
COPY ./streamx-console/streamx-console-service/target/streamx-console-service-*.tar.gz /
WORKDIR /
RUN tar zxvf streamx-console-service-*.tar.gz \
&& rm streamx-console-service-*.tar.gz \
&& mv streamx-console-service-* streamx
COPY ./deploy/k8s-conf/idc-dev-config streamx/plugins
COPY ./deploy/streamx/conf/hadoop-secret/bigdata.keytab streamx/conf
RUN mv streamx/plugins/idc-dev-config  streamx/plugins/config

FROM docker:dind
WORKDIR /streamx
COPY --from=deps-stage /streamx /streamx

ARG DB=localhost
ENV NODE_VERSION=16.1.0
ENV NPM_VERSION=7.11.2

RUN sed -i -e 's/eval $NOHUP/eval/' bin/streamx.sh \
    && sed -i -e 's/StreamXConsole \\/StreamXConsole/' bin/streamx.sh \
    && sed -i -e 's/>> "$APP_OUT" 2>&1 "&"//' bin/streamx.sh \
    && sed -i -e 's/localhost/'$DB'/' conf/application.yml \

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz \
    && tar -zxf hadoop-2.7.7.tar.gz -C / \
    && mv /hadoop-2.7.7 /hadoop \
    && rm hadoop-2.7.7.tar.gz

RUN apk add openjdk8 \
    && apk add maven \
    && apk add wget \
    && apk add vim \
    && apk add bash


ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
ENV MAVEN_HOME=/usr/share/java/maven-3
ENV HADOOP_HOME /hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV HADOOP_COMMON_LIB_NATIVE_DIR ${HADOOP_HOME}/lib/native
ENV YARN_CONF_DIR  $HADOOP_HOME/etc/hadoop
ENV HADOOP_OPTS "-Djava.library.path=$HADOOP_HOME/lib"
ENV HADOOP_CLASSPATH $HADOOP_HOME/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/tools/*:$HADOOP_HOME/share/hadoop/httpfs/*:$HADOOP_HOME/share/hadoop/kms/*:$HADOOP_HOME/share/hadoop/common/lib/*
ENV PATH "${PATH}:${HADOOP_HOME}/bin:$JAVA_HOME/bin:$MAVEN_HOME/bin"


RUN wget "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-x64.tar.gz" \
    && tar zxvf "node-v$NODE_VERSION-linux-x64.tar.gz" -C /usr/local --strip-components=1 \
    && rm "node-v$NODE_VERSION-linux-x64.tar.gz" \
    && ln -s /usr/local/bin/node /usr/local/bin/nodejs

RUN mkdir -p ~/.kube \
    && cat plugins/config >> ~/.kube/config

EXPOSE 10000
