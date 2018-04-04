# TW Northeast Tech Lead Meetup - Hands On Spark

## General Setup Instructions

* Install Homebrew (if not installed already)
  * `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
* Install xcode-select
  * `xcode-select --install`
* Install Java: `brew cask install java`
* Install Scala: `brew install scala`
* Install Apache Spark: `brew install apache-spark`
* Install sbt: `brew install sbt@1`
* Download the configuration jar "config-1.3.2.jar" from maven: http://central.maven.org/maven2/com/typesafe/config/1.3.2/ .
  Save this jar in this root directory.
* Install Python (need v3 or later): `brew install python`

## Kafka Producer Setup Instructions

* git clone https://github.com/ThoughtWorksInc/twde-capabilities
* `cd twde-capabilities/kafka-python-producer`
* Install Python packages: `pip3 install --upgrade -r requirements.txt`

## Spark Consumer Setup Instructions

* `cd twde-capabilities/tw-pipeline`
* Build and package: `sbt package`

## More Kafka Producer Setup Instructions
* Follow instructions from Kafka Producer README at
https://github.com/ThoughtWorksInc/twde-capabilities/tree/master/kafka-python-producer

## How to run (during session)
* Start consumer: spark-submit --jars config-1.3.2.jar --class com.thoughtworks.ca.de.streaming.KafkaConsumer --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 twde-capabilities/tw-pipeline/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
