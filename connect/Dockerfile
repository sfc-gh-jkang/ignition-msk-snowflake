# Kafka Connect worker with the Snowflake Connector for Kafka v4.
#
# Two runtime requirements, both of which fail at task start rather than at build time:
#  - glibc. v4 embeds a Rust-based Snowpipe Streaming SDK (a native .so built against glibc).
#    On musl images such as apache/kafka (Alpine) the task fails with
#    "Error loading shared library libgcc_s.so.1". So Kafka is copied out of the official image
#    onto a glibc JRE.
#  - Java 17+. Connector 4.2.0 bundles classes compiled for Java 17.
# Install docs: https://docs.snowflake.com/en/user-guide/kafka-connector/setup-kafka
ARG KAFKA_VERSION=3.9.1
FROM apache/kafka:${KAFKA_VERSION} AS kafka

FROM eclipse-temurin:21-jre-noble
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends curl gettext-base >/dev/null \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --create-home appuser
COPY --from=kafka /opt/kafka /opt/kafka
COPY plugin-jars.txt fetch_plugin.sh /tmp/plugin/
RUN bash /tmp/plugin/fetch_plugin.sh /opt/connect-plugins/snowflake-kafka-connector-4.2.0/lib \
 && chown -R appuser:appuser /opt/connect-plugins /opt/kafka
COPY --chmod=0755 start-connect.sh /usr/local/bin/start-connect.sh
USER appuser

# v4 allocates memory outside the JVM heap for its native SDK. Snowflake's guidance is to cap
# the heap at about half of the worker's memory.
ENV KAFKA_HEAP_OPTS="-Xms512m -Xmx1g"
EXPOSE 8083
ENTRYPOINT ["start-connect.sh"]
