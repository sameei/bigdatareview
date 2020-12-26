package xyz.sigmalab.kfch.util

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object KafkaUtil {

    def adminClient(server: String) = {
        val conf = {
            val p = new Properties()
            p.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, 2000) // 2.7.0
            // m.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, 2000) // 2.7.0
            p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server)
            p.put(AdminClientConfig.CLIENT_ID_CONFIG, "ADMIN_CLIENT")
            p
        }
        AdminClient.create(conf)
    }

    def newProducer(server: String): Producer[String, String] = {
        val props = {
            val p = new Properties()
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
            p.put(ProducerConfig.RETRIES_CONFIG, 0)
            p.put(ProducerConfig.ACKS_CONFIG, "all")
            p.put(ProducerConfig.CLIENT_ID_CONFIG, "PRODUCER_CLIENT")
            p
        }
        new KafkaProducer(props, new StringSerializer, new StringSerializer)
    }

    /**
     *
     * ERRORS: enable.auto.commit cannot be set to true when default group id (null) is used
     *
     * @param server
     * @param autoCommitIntervals
     * @param offsetReset "earliest" | "latest" | "none"
     * @return
     */
    def newConsumer(
        server: String,
        groupId: String,
        topic: String,
        autoCommitIntervals: Int = 1000,
        offsetReset: String = "earliest"
    ): Consumer[String, String] = {
        val props = {

            val p = new Properties()
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)

            if (autoCommitIntervals > 0) {
                p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)
                p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervals)
            } else p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)
            p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

            p.put(ConsumerConfig.CLIENT_ID_CONFIG, "CONSUMER_CLIENT")
            p
        }
        val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
        consumer.subscribe(Collections.singleton(topic))
        consumer
    }

    implicit class KafkaFtOpt[T](val self: KafkaFuture[T])
        extends AnyVal {
        def toScalaFuture: Future[T] = {
            val p = Promise[T]
            self.whenComplete { (t, error) =>
                if (error == null) p.success(t)
                else p.failure(error)
            }
            p.future
        }

        def toft: Future[T] = toScalaFuture

        def result: T = Await.result(toft, 30.seconds)

        def getIter[X](implicit ev: T <:< java.util.Collection[X]): Iterable[X] = {
            result.asInstanceOf[util.Collection[X]].asScala
        }

        def getMap[A, B](implicit ev: T <:< java.util.Map[A, B]): Map[A, B] = {
            result.asInstanceOf[util.Map[A, B]].asScala.toMap
        }
    }

    def consumeN[K, V](consumer: Consumer[K, V], n: Int, maxWait: Duration): Iterable[(K, V)] = {
        val jtimeout = java.time.Duration.ofMillis(maxWait.toMillis)
        val start = System.currentTimeMillis();
        val expectedEnd = start + maxWait.toMillis
        var cntu = true;
        var required = n;
        val buf = new ArrayBuffer[(K, V)](n)

        do {
            consumer.poll(jtimeout) match {
                case batch if batch.isEmpty =>
                    println(s"PolledBatch: EMPTY")
                case batch =>
                    println(s"PolledBatch: ${batch.count()}")
                    val takeN = if (batch.count() <= required) batch.count() else required
                    buf.appendAll(batch.iterator().asScala.take(takeN).map { i => i.key -> i.value })
                    required -= takeN
            }

            if (required == 0) cntu = false
            val diff = expectedEnd - System.currentTimeMillis()
            if (diff < 0) cntu = false
            println(s"Continue? ${required} / ${diff}")

        } while (cntu);

        buf
    }

    val NO_OFFSET_STRATEGY_EARLIEST = "earliest"
    val NO_OFFSET_STRATEGY_LATEST = "latest"
    val NO_OFFSET_STRATEGY_FAIL = "none"


    def debugOffsetAndConsumeN[K, V](
        server: String, topic: String, group: String,
        n: Int, maxWait: Duration,
        noOffsetStrategy: String = NO_OFFSET_STRATEGY_EARLIEST
    ) = {
        println("Connecting ...")
        val admin = adminClient(server)
        val consumer = newConsumer(server, group, topic, -1, noOffsetStrategy)

        println(s"Fetching Offsets of '${group}'")
        val offsets =
            admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata()
                .getMap



        if (offsets.isEmpty)
            println(s"NO OFFSET, Strategy: ${noOffsetStrategy}")
        else {
            offsets.foreach { case (k, v) => println(s"$k => $v") }
            println("Deleting Offsets ...")
            admin.deleteConsumerGroupOffsets(group, offsets.map(_._1).toSet.asJava).all().result
        }

        println(s"NO_OFFSET_STRATEGY: ${noOffsetStrategy}")

        consumeN(consumer, n, maxWait).foreach { i => println(s" :: $i")}
        consumer.commitSync()

        println(s"Fetching Offsets of '${group}'")
        val newOffsets =
            admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata()
                .getMap

        if (newOffsets.isEmpty) println("NO OFFSET !")
        else newOffsets.foreach { case (k, v) => println(s"$k => $v") }
    }

}
