FROM starrocks/allin1-ubuntu:4.1.4

USER root

COPY FixedSASTokenProvider.java /tmp/FixedSASTokenProvider.java

RUN mkdir -p /tmp/starrocks-sas-build \
    && javac \
      -cp /data/deploy/starrocks/be/lib/hadoop/common/hadoop-azure-3.4.3.jar:/data/deploy/starrocks/be/lib/hadoop/common/hadoop-common-3.4.3.jar \
      -d /tmp/starrocks-sas-build \
      /tmp/FixedSASTokenProvider.java \
    && jar uf /data/deploy/starrocks/be/lib/hadoop/common/hadoop-azure-3.4.3.jar \
      -C /tmp/starrocks-sas-build com/risingwave/starrocks/FixedSASTokenProvider.class \
    && jar uf /data/deploy/starrocks/fe/lib/hadoop-azure-3.4.3.jar \
      -C /tmp/starrocks-sas-build com/risingwave/starrocks/FixedSASTokenProvider.class \
    && rm -rf /tmp/starrocks-sas-build /tmp/FixedSASTokenProvider.java
