# apache-flink-playground

### Important Links
- https://mvnrepository.com/artifact/org.apache.flink
- https://ci.apache.org/projects/flink/flink-docs-release-1.9/getting-started/docker-playgrounds/flink-operations-playground.html
- ververica Flink training - https://training.ververica.com/
- NYC Taxi Data Repo - https://training.ververica.com/setup/taxiData.html

### Commands
```sh
# full install path on mac - /usr/local/Cellar/apache-flink/1.9.1
$ start-flink
$ stop-flink
$ flink run XYZ.jar
$ flink-config
# Logs / Output
$ tail -f /usr/local/Cellar/apache-flink/1.9.1/libexec/log/
```

#### Local Dashboard
- http://localhost:8081/#/overview


### Installation
- https://ci.apache.org/projects/flink/flink-docs-release-1.9/getting-started/tutorials/local_setup.html
- https://en.wikipedia.org/wiki/Netcat
```sh
$ brew install mvn
$ mvn --version
$ brew install apache-flink
$ flink --version
$ brew info apache-flink
$ brew install netcat
```


### Archetype to generate new project
- https://ci.apache.org/projects/flink/flink-docs-stable/dev/projectsetup/java_api_quickstart.html
```sh
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink             \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.10.0
```
