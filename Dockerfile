from enow/storm
MAINTAINER writtic <writtic@gmail.com>

ADD ./target/enow-storm-1.0.jar $STORM_HOME/extlib/enow-storm-1.0.jar
# ADD ./kafka_2.11-0.9.0.1.jar $STORM_HOME/lib/kafka_2.11-0.9.0.1.jar
# ADD ./storm-kafka-1.0.2.jar $STORM_HOME/lib/storm-kafka-1.0.2.jar
# ADD ./scala-library-2.11.8.jar $STORM_HOME/lib/scala-library-2.11.8.jar
WORKDIR /usr/share/storm

ADD ./target/enow-storm-1.0.jar topology.jar

# add startup script
ADD entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/usr/share/storm/entrypoint.sh"]
