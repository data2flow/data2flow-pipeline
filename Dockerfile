# data2flow-pipeline 실행 이미지. CI가 ./mvnw verify로 만든 jar를 넣는다.
# 비루트(UID 10001), TZ=UTC(저장·API는 UTC, 화면이 조직 시간대로 바꿈), 힙은 컨테이너 메모리의 75%(design/deployment.md §5.1)
FROM eclipse-temurin:21-jre-noble
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --no-create-home app
ENV TZ=UTC \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
WORKDIR /app
COPY target/data2flow-pipeline-*.jar /app/app.jar
USER 10001
EXPOSE 8080 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
