# 这是一个最简单的 Dockerfile 示例
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY . .
RUN cd sheetmind-mcp && mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/sheetmind-mcp/target/sheetmind-mcp-*-jar-with-dependencies.jar ./app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]