# Kafka Connect Connector for S3

## Background on Writing Connectors

[This article](https://opencredo.com/blogs/kafka-connect-source-connectors-a-detailed-guide-to-connecting-to-what-you-love/
) has a lot of useful context on how to write and deploy your own connector. 

[jcustonborder's github repo](https://github.com/jcustenborder/kafka-connect-transform-common) has more examples of custom connectors. 

## Building the RescueTime Custom Connector

This fork of Confluent's Kafka Connector project exists so that we
can drop in our own custom Single Message Transform (SMT) that trims
strings to lengths that can fit into Redshift's 65535-byte VARCHAR max.

To build it you need a boatload of dependencies that are laid out,
not quite correctly, at https://github.com/confluentinc/kafka-connect-storage-common/wiki/FAQ 
-- here are the steps that I followed successfully:

1. git clone git@github.com:confluentinc/common.git
1. cd common
1. mvn install -Dmaven.test.skip=true
1. cd ..
1. git clone git@github.com:confluentinc/kafka.git
1. cd kafka
1. Make sure this line is in build.gradle at the top:

    ```apply plugin: 'maven-publish'```

1. And make sure buildscript has mavenLocal():
    ```buildscript {
        repositories {
          mavenLocal()
          ...
    ```
1. gradle
1. ./gradlew build publishToMavenLocal -x test
1. cd ..
1. git clone git@github.com:confluentinc/rest-utils.git
1. cd rest-utils
1. mvn install -Dmaven.test.skip=true
1. cd ..
1. git clone git@github.com:confluentinc/schema-registry.git
1. cd schema-registry
1. mvn install -Dmaven.test.skip=true
1. cd ..
1. git clone git@github.com:confluentinc/kafka-connect-storage-common.git
1. cd kafka-connect-storage-common
1. mvn install -Dmaven.test.skip=true
1. cd ..
1. git clone git@github.com:RescueTime/kafka-connect-storage-cloud.git (this project home)
1. cd kafka-connect-storage-cloud
1. mvn install -Dmaven.test.skip=true
1. cp ./kafka-connect-s3/target/kafka-connect-s3-5.4.0-SNAPSHOT.jar ~/confluent-5.3.0/share/java/kafka-connect-s3/ 
1. ~/confluent-5.3.0/bin/confluent local stop (if already running)
1. ~/confluent-5.3.0/bin/confluent local start

The last step assumes you have the developer version of Confluent
installed in your home directory. If not, get it here: https://docs.confluent.io/current/quickstart/index.html

Once ready to ship the jar, put it in the rt-playbooks project, which is how it gets on the Kafka brokers:
1. cp ./kafka-connect-s3/target/kafka-connect-s3-5.4.0-SNAPSHOT.jar ~/dev/rt-playbooks/kafka/roles/confluent.kafka_connect/files/ 

Then run the appropriate ansible playbook to deploy Kafka. 
# Confluent's Readme Follows

*kafka-connect-storage-cloud* is the repository for Confluent's [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data from Kafka into Amazon S3. 

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

Blogpost for this connector can be found [here](https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once).

# Development

To build a development version you'll need a recent version of Kafka 
as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch.
See [the kafka-connect-storage-common FAQ](https://github.com/confluentinc/kafka-connect-storage-common/wiki/FAQ)
for guidance on this process.

You can build *kafka-connect-storage-cloud* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-storage-cloud
- Issue Tracker: https://github.com/confluentinc/kafka-connect-storage-cloud/issues


# License

This project is licensed under the [Confluent Community License](LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=large)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_large)
