# Distributed System 1 Project

This repository concerns all about the Distributed System 1 project employing [Java 8](https://www.oracle.com/java/technologies/java8.html).

## Members

|  Name    |  Surname   |     Username        |
| :------: | :--------: | :-----------------: |
| Samuele  | Bortolotti | `samuelebortolotti `|
| Luca     | De Menego  | `lucademenego99`    |

## Set Up

You can either use [Gradle](https://gradle.org/) to compile the code or you can run it in a [Docker](https://www.docker.com/) container.

### Gradle

> For all the tasks, you can run `gradle tasks`

Gradle is used to both keep track of the dependencies of the project and compiling it.

#### Build

To compile the project you can run:

```bash
$ gradle clean build
```

#### Run

To run the project you can use:

```bash
$ gradle run
```

Or run directly the produced jar file:

```bash
$ java -jar build/libs/DS1-project-1.0-VERSION.jar
```

#### Documentation

Along with the report, you can generate the documentation of the project running the following gradle task

```bash
$ gradle javadoc
```

The documentation will be provided in the `build/docs/javadoc` folder.
You can open it using your favourite browser just by typing:

```bash
$ xdg-open build/docs/javadoc/index.html
```

#### Tests

The code comes along with some [JUnit](https://junit.org/junit5/docs/current/user-guide/) tests, you can run them with the following line:

```bash
$ gradle test
```

Or alternatively, they are ran toghether with the build command, therefore either after a `gradle build` or a `gradle test` you can find the tests result in the `build/reports/tests/test/` folder.

Again, you can consult them opening the `index.html` file with your browser.

```bash
$ xdg-open build/reports/tests/test/index.html 
```

### Docker

The image generated with the Dockerfile uses a mutli-stage build so as to limit as much as possible the image size, by first compiling the code with a Gradle image and then run it using a jre one.

#### Docker build

To build the image you can run:

```bash
$ docker build -t ds1:latest .
```

#### Docker run

To run the application you can run:

```bash
$ docker run ds1:latest
```
