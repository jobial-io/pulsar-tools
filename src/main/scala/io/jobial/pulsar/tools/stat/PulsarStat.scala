package io.jobial.pulsar.tools.stat

import cats.effect.IO
import cats.effect.IO.sleep
import io.jobial.pulsar.admin.PulsarAdminContext
import io.jobial.pulsar.admin.PulsarAdminUtils
import io.jobial.sclap.CommandLineApp
import org.apache.pulsar.client.admin.PulsarAdmin

import java.net.InetAddress
import java.time.Instant.ofEpochMilli
import java.time.LocalDateTime.now
import java.time.format.DateTimeFormatter.ofPattern
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.util.Try

object PulsarStat extends CommandLineApp with PulsarAdminUtils {

  def run =
    for {
      url <- opt[String]("url", "u").default(PulsarAdminContext().url)
      namespace <- opt[String]("namespace", "n").default(".*")
      context = PulsarAdminContext(url, namespace)
      r <- run(context)
    } yield r

  def run(implicit context: PulsarAdminContext) =
    for {
      listTenants <- listTenants
      listNamespaces <- listNamespaces
      listTopics <- listTopics
      listSubscriptions <- listSubscriptions
      listConsumers <- listConsumers
      listProducers <- listProducers
      stats <- stats
    } yield
      stats orElse
        listTenants orElse
        listNamespaces orElse
        listTopics orElse
        listSubscriptions orElse
        listConsumers orElse
        listProducers orElse
        printHeaderAndStatLines

  def listTenants(implicit context: PulsarAdminContext) =
    subcommand("tenants") {
      for {
        tenants <- context.admin.use { implicit admin =>
          tenants
        }
      } yield tenants.map(_.name).sorted.map(println)
    }

  def listNamespaces(implicit context: PulsarAdminContext) =
    subcommand("namespaces") {
      for {
        namespaces <- context.admin.use { implicit admin =>
          namespaces(context.namespace)
        }
      } yield namespaces.map(_.name).sorted.map(println)
    }

  def listTopics(implicit context: PulsarAdminContext) =
    subcommand("topics") {
      for {
        topics <- context.admin.use { implicit admin =>
          topics(context.namespace)
        }
      } yield topics.map(_.name).sorted.map(println)
    }

  def listSubscriptions(implicit context: PulsarAdminContext) =
    subcommand("subscriptions") {
      for {
        subscriptions <- context.admin.use { implicit admin =>
          subscriptions(context.namespace)
        }
        _ <- IO(println(f"${"Name"}%46s${"Topics"}%7s"))
      } yield for {
        s <- subscriptions.sortBy(_.name)
      } yield {
        val name = s.name
        val topics = s.topics.map(_.name).sorted.mkString(" ")
        println(f"${name}%46s ${topics}")
      }
    }

  def listConsumers(implicit context: PulsarAdminContext) =
    subcommand("consumers") {
      for {
        consumers <- context.admin.use { implicit admin =>
          consumers(context.namespace)
        }
        lines <- {
          for {
            consumer <- consumers.sortBy(_.getConsumerName)
          } yield IO {
            val address = consumer.getAddress
            val hostname = resolveHostname(address.substring(1, address.indexOf(':')))
            val port = address.substring(address.indexOf(':') + 1)
            val lastConsumedTimestamp =
              if (consumer.getLastConsumedTimestamp > 0)
                ofEpochMilli(consumer.getLastConsumedTimestamp)
              else
                ""
            val consumerName = if (consumer.getConsumerName === "") "<unnamed>" else consumer.getConsumerName
            val msgOutCounter = consumer.getMsgOutCounter

            f"${consumerName}%10s${hostname.map(_ + s":$port").getOrElse(address)}%40s${lastConsumedTimestamp}%25s${msgOutCounter}%12d"
          }
        }.parSequence
        _ <- IO(println(f"${"Name"}%10s${"Address"}%40s${"LastConsumed"}%25s${"MsgOutCount"}%12s"))
      } yield for {
        l <- lines
      } yield println(l)
    }

  def listProducers(implicit context: PulsarAdminContext) =
    subcommand("producers") {
      for {
        publishers <- context.admin.use { implicit admin =>
          publishers(context.namespace)
        }
        lines <- {
          for {
            publisher <- publishers.sortBy(_.getProducerName)
          } yield IO {
            val address = publisher.getAddress
            val hostname = resolveHostname(address.substring(1, address.indexOf(':')))
            val port = address.substring(address.indexOf(':') + 1)
            val connectedSince = publisher.getConnectedSince.substring(0, publisher.getConnectedSince.indexOf('.'))
            val producerName = if (publisher.getProducerName === "") "<unnamed>" else publisher.getProducerName
            val msgRateIn = publisher.getMsgRateIn

            f"${producerName}%45s${hostname.map(_ + s":$port").getOrElse(address)}%40s${connectedSince}%20s${msgRateIn}%10.2f"
          }
        }.parSequence
        _ <- IO(println(f"${"Name"}%45s${"Address"}%40s${"Since"}%20s${"MsgRateIn"}%10s"))
      } yield for {
        l <- lines
      } yield println(l)
    }

  def stats(implicit context: PulsarAdminContext) =
    subcommand("stats").description("(default)") {
      printHeaderAndStatLines
    }

  def resolveHostname(address: String) =
    Try(InetAddress.getByName(address)).map(_.getHostName).toOption

  def statLine(implicit admin: PulsarAdmin, context: PulsarAdminContext) =
    for {
      topics <- topics(context.namespace)
      subscriptions <- subscriptions(context.namespace)
      stats <- topics.map(_.stats).parSequence
      inRate = stats.map(_.getMsgRateIn).sum
      outRate = stats.map(_.getMsgRateOut).sum
      msgThroughputIn = stats.map(_.getMsgThroughputIn).sum
      msgThroughputOut = stats.map(_.getMsgThroughputOut).sum
      backlogSize = stats.map(_.getBacklogSize).sum
      storageSize = stats.map(_.getStorageSize).sum
      producers = stats.map(_.getPublishers.asScala.toList).flatten
      subStats = stats.map(_.getSubscriptions.asScala.toList).flatten
      consumers = subStats.flatMap(_._2.getConsumers.asScala.toList)
    } yield StatLine(
      topics.size,
      subscriptions.size,
      consumers.size,
      producers.size,
      inRate,
      outRate,
      msgThroughputIn,
      msgThroughputOut,
      backlogSize,
      storageSize
    )

  def printStatLines(implicit admin: PulsarAdmin, context: PulsarAdminContext): IO[Unit] = {
    for {
      line <- statLine
      _ <- IO(println(line.print))
      _ <- sleep(15.seconds)
      _ <- printStatLines
    } yield ()
  } handleErrorWith { t =>
    sleep(15.seconds) >>
      printStatLines
  }

  def printHeaderAndStatLines(implicit context: PulsarAdminContext) =
    context.admin.use { implicit admin =>
      for {
        namespaces <- namespaces(context.namespace)
        _ <- if (namespaces.size === 1 && namespaces.head.name === context.namespace) IO {
          println(s"Showing statistics for ${context.url} and namespace ${context.namespace}")
        } else IO {
          println(s"Showing statistics for ${context.url} and namespace pattern ${context.namespace}")
          println("Matching namespaces:")
          println(namespaces.map(_.name).mkString("", "\n", "\n"))
        }
        _ <- IO(println(StatLine.printHeader))
        _ <- printStatLines
      } yield ()
    }
}

case class StatLine(
  topics: Int,
  subscriptions: Int,
  consumers: Int,
  producers: Int,
  inRate: Double,
  outRate: Double,
  msgThroughputIn: Double,
  msgThroughputOut: Double,
  backlogSize: Long,
  storageSize: Long
) {

  val print =
    f"${ofPattern("yyyyMMdd-HHmmss").format(now)}%15s${topics}%8s${subscriptions}%8s" +
      f"${consumers}%8s${producers}%8s" +
      f"${inRate}%11.2f${outRate}%11.2f" +
      f"${msgThroughputIn / 1024 / 1024}%11.2f${msgThroughputOut / 1024 / 1024}%11.2f" +
      f"${backlogSize / 1024 / 1024}%10d${storageSize / 1024 / 1024}%10d"
}

object StatLine {
  val printHeader =
    f"${"Timestamp"}%15s${"Topic"}%8s${"Subs"}%8s${"Cons"}%8s${"Prod"}%8s${"MsgRateIn"}%11s" +
      f"${"MsgRateOut"}%11s${"TputInMB"}%11s${"TputOutMB"}%11s" +
      f"${"BacklogMB"}%10s${"StorageMB"}%10s"
}

