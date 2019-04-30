FROM azul/zulu-openjdk-alpine:11 as builder

RUN apk add --no-cache curl tar bash
ARG MAVEN_VERSION=3.5.4
RUN mkdir -p /usr/share/maven && \
  curl -fsSL http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar -xzC /usr/share/maven --strip-components=1 && \
  ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

# make source folder
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
# download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline
# copy source files (keep in image)
COPY src /usr/src/app/src
# create jar file (keep in image)
RUN mvn package -U -DskipTests


FROM azul/zulu-openjdk-alpine:11
ARG JAR_FILE=cassandra-tester-0.0.1-SNAPSHOT.jar
COPY --from=builder /usr/src/app/target/$JAR_FILE app.jar

#healthcheck
RUN apk add --no-cache curl
HEALTHCHECK --interval=15s --timeout=5s CMD curl -f --silent http://localhost:8080/actuator/health 2>&1 | grep -q '{"status":"UP"}' || exit 1

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]