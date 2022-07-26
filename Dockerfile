# ===== Build the application =====
FROM gradle:7.4.0-jdk8 AS build
# run as gradle user 
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build -x test

# ===== Run the application =====
FROM openjdk:8-jre-slim
# Working directory
WORKDIR /app
# Copy the built artifacts
COPY --from=build /home/gradle/src/build/libs/*.jar /app/main.jar
# create group and user
RUN groupadd -r java-user && useradd -g java-user java-user
# set ownership and permissions
RUN chown -R java-user:java-user /app
# switch to user
USER java-user
# Run the command
CMD ["java", "-jar", "/app/main.jar"]
