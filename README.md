# CSV Import Tutorial: Spring Boot + Apache Spark 4.2.0 + Kafka

A practical, heavily-commented tutorial project showing how to combine three
technologies that are commonly used together in real data pipelines:

- **Spring Boot** — REST API + dependency injection + configuration
- **Apache Spark 4.2.0** — reads/cleans/transforms the CSV file
- **Apache Kafka** — streams the cleaned records to any downstream consumer

## Architecture / Data Flow

```
 Client                Controller              Service (Spark)         Kafka
   │  POST /api/csv/upload   │                        │                  │
   ├────────────────────────►│                        │                  │
   │                         │ save temp file          │                  │
   │                         ├───────────────────────►│                  │
   │                         │                         │ read CSV         │
   │                         │                         │ clean/filter     │
   │                         │                         │ (Spark DataFrame)│
   │                         │                         ├─────────────────►│  (CsvRecordProducer)
   │                         │◄────────────────────────┤  publish rows    │
   │◄────────────────────────┤  publishedCount          │                  │
   │                         │                         │                  │
   │                         │                         │   @KafkaListener │
   │                         │                         │◄─────────────────┤  (CsvRecordConsumer)
   │                         │                         │  "process" row   │
```

## Project Layout

```
src/main/java/com/tutorial/csvsparkkafka/
├── CsvSparkKafkaApplication.java   # Spring Boot entry point
├── config/
│   ├── SparkConfig.java            # SparkSession bean (local[*] mode)
│   └── KafkaTopicConfig.java       # Declares the Kafka topic
├── controller/
│   └── CsvUploadController.java    # REST endpoints (/upload, /status)
├── service/
│   └── CsvImportService.java       # Spark read + clean + transform logic
├── kafka/
│   ├── CsvRecordProducer.java      # Publishes Employee -> JSON -> Kafka
│   └── CsvRecordConsumer.java      # @KafkaListener that reads messages back
└── model/
    └── Employee.java               # POJO representing one CSV row
src/main/resources/
├── application.yml                 # Spring/Kafka/Spark configuration
└── sample-data/employees.csv       # Sample file to test with (includes 2 "bad" rows on purpose)
```

## Prerequisites

- **JDK 17+** (Spark 4.x requires it)
- **Maven 3.9+**
- **A running Kafka broker** on `localhost:9092` (see below for the fastest way to get one)

### Fastest way to get Kafka running locally (Docker)

```bash
docker compose up -d
```

Or the equivalent one-liner:

```bash
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  apache/kafka:3.7.0
```

(Any Kafka image/version you already have will work — this project only needs
a broker reachable at `localhost:9092`. Change `spring.kafka.bootstrap-servers`
in `application.yml` if yours runs elsewhere.)

## Running the app

Because Spark 4.x reaches into JDK internals via reflection, you must pass
`--add-opens` flags on JDK 17+. `mvn spring-boot:run` already injects them
(see `spark.jvm.args` in `pom.xml`). The one that is easy to miss — and the
one that breaks CSV date inference — is `java.base/sun.util.calendar`.

If you run the main class from an IDE, add these VM options:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

```bash
mvn spring-boot:run
```

If you instead run the packaged jar directly, pass the same flags to `java`:

```bash
mvn clean package -DskipTests
java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/java.io=ALL-UNNAMED \
     --add-opens=java.base/java.net=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
     --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
     --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
     --add-opens=java.base/sun.security.action=ALL-UNNAMED \
     --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
     -jar target/csv-spark-kafka-tutorial.jar
```

## Trying it out

Upload the bundled sample CSV:

```bash
curl -F "file=@src/main/resources/sample-data/employees.csv" \
     http://localhost:8080/api/csv/upload
```

Expected response — note it publishes **5**, not 7, records: the sample file
intentionally includes one row with a missing `name` and one row with a
negative `salary`, both of which `CsvImportService` filters out with Spark's
`.na().drop()` and `.filter(col("salary").gt(0))` calls. Check the console
logs to see Spark's inferred schema and the before/after row counts.

```json
{
  "message": "CSV processed and published to Kafka",
  "recordsPublished": 5
}
```

Check how many records the Kafka consumer has processed asynchronously:

```bash
curl http://localhost:8080/api/csv/status
```

## Key things this tutorial demonstrates

1. **Spark as a CSV cleaning/transformation engine** — schema inference,
   `.na().drop()`, `.filter()`, `.select()` (see `CsvImportService`).
2. **Decoupling import from processing via Kafka** — the producer doesn't
   know or care what the consumer does with each record.
3. **Keying Kafka messages meaningfully** — using `employee.id` as the
   message key so all events for one employee stay ordered on one partition.
4. **Avoiding dependency hell** — the `pom.xml` excludes Spark's bundled
   Jackson so it doesn't collide with Spring Boot's.
5. **Auto-creating Kafka topics** — `KafkaTopicConfig` + Spring's `KafkaAdmin`
   creates `csv-employee-records` for you on startup.

## Extending this tutorial

- Swap the in-memory `AtomicLong` counter in `CsvRecordConsumer` for a real
  database write (Spring Data JPA, MongoDB, etc.).
- Replace `local[*]` Spark mode with a real cluster URL
  (`spark://host:7077` or a Kubernetes master) once your CSVs get large.
- Add a Dead Letter Topic for messages that fail JSON parsing in the consumer.
- Add validation annotations (`@NotBlank`, `@Positive`) plus a `@ControllerAdvice`
  for cleaner error responses on bad uploads.
