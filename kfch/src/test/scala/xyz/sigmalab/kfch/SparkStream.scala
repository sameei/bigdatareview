package xyz.sigmalab.kfch

import java.util.Collections

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.scalatest.matchers.must.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import xyz.sigmalab.kfch.etc.{KafkaSparkCases, KafkaSparkUtil, KafkaUtil}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import xyz.sigmalab.kfch.util.KafkaUtil._

class SparkStream
    extends AnyFlatSpec with Matchers {

    val kafkaServer = "127.0.0.1:9092"

    def newName(s: String) =
        s"${classOf[SparkStream].getCanonicalName}-${s}-${System.currentTimeMillis()}"

    it must "RUN01" in {
        KafkaSparkCases.firstCase("first-case", kafkaServer, KafkaUtil.NoOffsetStrategy.EARLIEST)
    }

    it must "RUN02" in {
        KafkaSparkCases.firstCase("first-case", kafkaServer, KafkaUtil.NoOffsetStrategy.LATEST)
    }


}
