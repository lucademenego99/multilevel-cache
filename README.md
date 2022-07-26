# Distributed System 1 Project

This repository concerns all about the Distributed System 1 project employing [Java 8](https://www.oracle.com/java/technologies/java8.html) and [Akka](https://akka.io/).

The project entails putting a **distributed multi-level cache** protocol into practice.
Due to the fact that caches store frequently requested objects and respond to the majority of client requests independently, their main purpose is to avoid database congestion.
The Akka actors involved are clients, caches, and a single database.

Multiple clients that can read and write data stored in a database make up the system.
The system has been designed with read operations in mind, which are instantly handled by the cache layer located near to the clients.
As an alternative, write operations always use the primary database. Clients interacting with the same cache are assured _not_ to receive outdated values once their write operations have been validated, 
and the protocol should offer **eventual consistency**.
Additionally, two "critical" variations of the fundamental operations, namely _CRITREAD_ and _CRITWRITE_, which offer better assurances.

By assumption, the database store remains constant during the protocol's execution.
> For the sake of simplicity the data stored in databases and caches is made up of integer numbers.

Moreover, according to the project statement, caches come with an initial **tree structure** which needs to be specified.

Some caches operate at the first level, with a direct connection to the database. We refer to those as **L1** caches. 
L1 caches are the parents of **L2** caches in the tree. The latter interact with clients, receiving their requests and responding with the results.

The provided operations are:

- **READ**: If the requested value is present in memory when an L2 cache gets a Read, it responds right away with it.
If not, it will get in touch with the parent L1 cache.
The main database may be contacted or the L1 cache may respond with the value (typically referred to as read-through mode).
Until the customer is contacted, responses travel backward along the path of the request.
Caches store the item on the way back for upcoming requests.
Client timeouts ought to account for how long it takes a request to get to the database.
- **WRITE**: The request is sent to the database, which executes the write operation and notifies all of its L1 caches of the update.
It is then sent to all associated L2 caches by all L1 caches.
This might theoretically apply the change to every cache, which is required for eventual consistency.
The written item will only be updated in caches that previously had it stored, so take note of this.
- **CRITREAD**: Fetches the most recent value from the database, however unlike a read, the request is transmitted even if the item is already in the L2 or L1 cache.
- **CRITWRITE**: The same as in Write, the request is sent to the database.
However, the database must make sure that no cache contains an outdated value for the written item before the write operation is applied.
No client should be able to read the new value from any cache, followed by the old value.
The database propagates the change as for Write once it has confirmed that the cached objects have been deleted.

A note on crashes and recoveries:

Caches may crash working at critical algorithmic points.
The system need to use a straightforward timeout-based crash detection mechanism, as demonstrated in the labs.
When a client notices that one L2 cache has crashed, it will choose another L2 cache and reroute its requests.
The primary database will be chosen as the parent by an L2 cache that notices that its L1 parent has crashed.
Caches lose all stored objects when they crash (we presume they were saved in volatile memory).
They do, however, still preserve information about the system, such as the database actor and the ActorRef of their tree neighbors.
After a set amount of time, caches recover and restart functioning.

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
gradle clean build -x test
```

> The -x test flag is used in order to avoid the test execution at compile time. 
> Usually, the test execution is a good idea, however the requested time for the test to complete is around 10 minutes, which may be annoying.

#### Run

To run the project you can use either the gradle command:

```bash
gradle run --args="--l1 5 --l2 3 --clients 1 --seconds 25"
```

or you can run directly the produced jar file:

```bash
java -jar build/libs/DS1-project-1.0-VERSION.jar --l1 5 --l2 3 --clients 1 --seconds 25
```

int countL1 = 5;
int countL2 = 5;
int countClients = 3;
int secondsForIteration = 20;

where the command line arguments are:

- clients <Number of clients>: Number of clients connected to the hierarchical distributed cache [default 5]
- l1 <Number of l1 caches>: Number of L1 caches which will be present in the hierarchical distributed cache
- l2 <Number of l2 caches>: Number of L2 caches which will be present in the hierarchical distributed cache
- seconds <Number of seconds per iteration>: Number of seconds each iteration takes

> For more information run either:
> 
> `gradle run --args="--help"`
> 
> or:
> 
> `java -jar build/libs/DS1-project-1.0-VERSION.jar --help`

#### Documentation

Along with the report, you can generate the documentation of the project running the following gradle task

```bash
gradle javadoc
```

The documentation will be provided in the `build/docs/javadoc` folder.
You can open it using your favourite browser just by typing:

```bash
xdg-open build/docs/javadoc/index.html
```

#### Tests

The code comes along with some [JUnit](https://junit.org/junit5/docs/current/user-guide/) tests, you can run them with the following line:

```bash
$ gradle test
```

Or alternatively, they are run together with the build command, therefore either after a `gradle build` or a `gradle test` you can find the tests result in the `build/reports/tests/test/` folder.

Again, you can consult them opening the `index.html` file with your browser.

```bash
xdg-open build/reports/tests/test/index.html 
```

### Docker

The image generated with the Dockerfile uses a multi-stage build in order to limit as much as possible the image size, by first compiling the code with a Gradle image and then run it using a jre one.

#### Docker build

To build the image you can run:

```bash
docker build -t ds1:latest .
```

#### Docker run

To run the application you can run:

```bash
docker run ds1:latest
```