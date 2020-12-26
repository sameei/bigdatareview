package xyz.sigmalab.kfch

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import java.util.Collections

import org.apache.kafka.clients.admin.{CreateTopicsOptions, NewTopic}
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaEssentials
    extends AnyFlatSpec with Matchers {

    val kafkaServer = "127.0.0.1:9092"
    val topicName = "topic"
    val groupId = "group"
    val pollTimeout = java.time.Duration.ofSeconds(10)

    import util.KafkaUtil._

    lazy val admin = adminClient(kafkaServer)
    lazy val producer = newProducer(kafkaServer)
    // lazy val consumer = newConsumer(kafkaServer, groupId)

    ignore must "creat a topic and retrieve it in list" in {
        val topicConfig = admin.createTopics(
            Collections.singleton(new NewTopic(topicName, 1, 1.asInstanceOf[Short])),
            new CreateTopicsOptions
        ).config(topicName).result
        topicConfig.entries().forEach(i => println(i))
    }

    ignore must "publish into topic" in {
        (1 to 10).map(i => f"Message-${i}%-5s").foreach { i =>
            producer.send(new ProducerRecord(topicName, "?", i)).get()
        }
    }

    ignore must "consumes from topic" in {
        consumeN(newConsumer(kafkaServer, groupId + "q", topicName), 10, 10.seconds).foreach { i =>
            println(s"HERE : ${i}")
        }
    }

    ignore must "get offsets" in {

        val offsets =
            admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .getMap

        offsets.foreach { case (k,v) =>  println(s"$k, $v") }
    }

    it must "debug" in {
        debugOffsetAndConsumeN(kafkaServer, topicName, groupId, 10, 10.seconds, NO_OFFSET_STRATEGY_LATEST)
        println("DONE ===\n")
        debugOffsetAndConsumeN(kafkaServer, topicName, groupId + "x", 10, 10.seconds, NO_OFFSET_STRATEGY_LATEST)
        println("DONE ===\n")
    }

}
