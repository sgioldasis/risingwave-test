# Hive Metastore shim for the StarRocks Delta federation demo (serving-layer
# doc §13): StarRocks's Delta catalog requires an HMS metastore with no
# filesystem-only mode, and HMS validates table locations through Hadoop's
# FileSystem — which has no Azure classes in this image. abfs-shim.jar (source
# in shim/, built against the FE container's hadoop-common, Java 8 target) is
# the pass-through that keeps abfss URIs intact; core-site.xml wires it in.
# Rebuild the jar after editing the source (from the repo root):
#   docker cp hms/shim starrocks:/tmp/shim && \
#   docker exec starrocks sh -c 'cd /tmp/shim && \
#     javac --release 8 -cp /opt/starrocks/fe/lib/hadoop-common-3.4.3.jar \
#     shim/AbfsPassthroughFs.java && jar cf abfs-shim.jar shim/AbfsPassthroughFs.class' && \
#   docker cp starrocks:/tmp/shim/abfs-shim.jar hms/abfs-shim.jar
FROM apache/hive:4.0.1
COPY abfs-shim.jar /opt/hive/lib/
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
