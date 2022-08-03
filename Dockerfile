FROM openjdk:8-alpine
WORKDIR /
COPY target/croct-desafio-1.0-SNAPSHOT.jar /
CMD ["java", "-jar","croct-desafio-1.0-SNAPSHOT.jar"]