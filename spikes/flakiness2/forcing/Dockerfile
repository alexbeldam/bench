FROM mongodb/mongodb-atlas-local:8.0
USER root
COPY scripts/mongod-wrap /usr/local/bin/mongod
COPY scripts/java-wrap /opt/mongot/bin/jdk/bin/java.wrap
RUN mv /opt/mongot/bin/jdk/bin/java /opt/mongot/bin/jdk/bin/java.real \
 && mv /opt/mongot/bin/jdk/bin/java.wrap /opt/mongot/bin/jdk/bin/java \
 && chmod 0755 /usr/local/bin/mongod /opt/mongot/bin/jdk/bin/java
USER mongod
