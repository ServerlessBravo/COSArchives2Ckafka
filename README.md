### Forward logs on COS to CKafka


A application for unzipping `.tar.gz` files from `COS` and forwarding the messages to Ckafka, then `Clickhouse` can create the table with Kafka connection

### Change Configuration

App.java:

```java
private static final String REGION_NAME = "ap-guangzhou";
private static final String SECRET_ID = "<replace by secret id>";
private static final String SECRET_KEY = "<replace by secret key>";
private static final String TARGET_BUCKET = "<replace_by_bucket_name>";
private static final String APP_ID = "<replace by account appid>";     

```

`resources/ckafka_client_jaas.conf` configure Ckafka credentials

```java
KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="<username: eg. ckafka-xxxx#test-1>" 
  password="<password>";
};

```

`resources/kafka.properties` configure address of Ckafka

```java
bootstrap.servers=ckafka-xxxx.ap-guangzhou.ckafka.tencentcloudmq.com:6002
topic=<topic>
group.id=<group_id>
java.security.auth.login.config.plain=app/src/main/resources/ckafka_client_jaas.conf
max.message.bytes=15728640
```

Artifacts:

```
➜  Java8-ETL git:(main) ✗ tree app/build/libs
app/build/libs
├── app-all.jar
└── app.jar

0 directories, 2 files

```

### Deploy to SCF

deploy the file `app-all.jar`
