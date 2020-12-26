package xyz.sigmalab.kfch.launcher

import java.util
import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, KafkaAdminClient, ListTopicsOptions}
import org.apache.kafka.common.KafkaFuture
import scopt.OParser

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import xyz.sigmalab.kfch.util.KafkaUtil._

object CheckTopic {

    /*
    cd /Volumes/Extra/Workspace/confluent-6.0.0
    export CONFLUENT_HOME=$(pwd)
    ./bin/confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
    ./bin/confluent local services start
    https://docs.confluent.io/platform/current/quickstart/ce-quickstart.html
    */

    sealed trait Config

    final case object ConfigZero
        extends Config

    final case class LsTopics(server: String = "?")
        extends Config

    final case class GroupOffsets(server: String = "?", group: String = "?")
        extends Config

    val options = {
        val builder = OParser.builder[Config]
        import builder._
        OParser.sequence(
            programName("CheckTopic"),
            cmd("lstp")
                .text("List all topics")
                .action { (u, c) => LsTopics() }
                .children(
                    opt[String]('s', "server")
                        .action { (s, c) => c.asInstanceOf[LsTopics].copy(server = s) }
                        .required()
                ),
            cmd("lsgf")
                .text("List Offsets of a Group")
                .action { (u, c) => GroupOffsets() }
                .children(
                    opt[String]('s', "server")
                        .action { (s, c) => c.asInstanceOf[GroupOffsets].copy(server = s) }
                        .required(),
                    opt[String]('g', "group")
                        .action { (g, c) => c.asInstanceOf[GroupOffsets].copy(group = g) }
                        .required()
                )
        )
    }

    def main(args: Array[String]): Unit =
        // process(args)
        process(Array("lstp", "-s", "127.0.0.1:9092"))

    def process(args: Array[String]): Unit = {
        OParser.parse(options, args, ConfigZero) match {
            case Some(LsTopics(server)) =>
                adminClient(server).listTopics().listings().getIter.foreach { t =>
                    println(s"TopicListing: $t")
                }
            case Some(GroupOffsets(server, group)) =>
                adminClient(server)
                    .listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata()
                    .getMap.foreach { case (k, v) => println(s"$k => $v")}
            case Some(ConfigZero) | None =>
                println("NO COMMAND")
        }
    }


}
