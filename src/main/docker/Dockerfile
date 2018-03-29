FROM frolvlad/alpine-oraclejdk8:8.141.15-full
MAINTAINER lee <licheng@aoneai.com>
ARG finalName
ADD $finalName /app.jar
ENTRYPOINT ["java", "-jar","/app.jar","--spring.config.location=${CONFIG_LOCATION}"]