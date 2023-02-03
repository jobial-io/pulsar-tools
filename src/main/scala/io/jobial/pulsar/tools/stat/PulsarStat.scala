package io.jobial.pulsar.tools.stat

import cats.effect.IO
import cats.effect.IO.sleep
import io.jobial.pulsar.admin.PulsarAdminContext
import io.jobial.pulsar.admin.PulsarAdminUtils
import io.jobial.sclap.CommandLineApp
import org.apache.pulsar.client.admin.PulsarAdmin

import java.time.LocalDateTime.now
import java.time.format.DateTimeFormatter.ofPattern
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

object PulsarStat extends CommandLineApp with PulsarAdminUtils {

  def run =
    for {
      url <- opt[String]("url", "u").default(PulsarAdminContext().url)
      namespace <- opt[String]("namespace", "n").default(".*")
      context = PulsarAdminContext(url, namespace)
      listTenants <- listTenants(context)
      listNamespaces <- listNamespaces(context)
      listTopics <- listTopics(context)
      listSubscriptions <- listSubscriptions(context)
    } yield
      listTenants orElse
        listNamespaces orElse
        listTopics orElse
        listSubscriptions orElse
        printHeaderAndStatLines(context)

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
          namespaces
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
      } yield subscriptions.sortBy(_.name).map(println)
    }

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
      producers = stats.map(_.getPublishers.asScala).flatten
      subStats = stats.map(_.getSubscriptions.asScala).flatten
      consumers = subStats.flatMap(_._2.getConsumers.asScala)
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

  def printStatLines(implicit admin: PulsarAdmin, context: PulsarAdminContext): IO[Unit] =
    for {
      line <- statLine
      _ <- IO(println(line.print))
      _ <- sleep(15.seconds)
      _ <- printStatLines
    } yield ()

  def printHeaderAndStatLines(implicit context: PulsarAdminContext): IO[Unit] =
    context.admin.use { implicit admin =>
      for {
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

  def print =
    f"${ofPattern("yyyyMMdd-HHmmss").format(now)}%15s${topics}%8s${subscriptions}%8s" +
      f"${consumers}%8s${producers}%8s" +
      f"${inRate}%11.2f${outRate}%11.2f" +
      f"${msgThroughputIn / 1024 / 1024}%11.2f${msgThroughputOut / 1024 / 1024}%11.2f" +
      f"${backlogSize / 1024 / 1024}%10d${storageSize / 1024 / 1024}%10d"
}

object StatLine {
  def printHeader =
    f"${"Timestamp"}%15s${"Topics"}%8s${"Subs"}%8s${"Cons"}%8s${"Prods"}%8s${"MsgRateIn"}%11s" +
      f"${"MsgRateOut"}%11s${"ThptInMB"}%11s${"ThrptOutMB"}%11s" +
      f"${"BacklogMB"}%10s${"StorageMB"}%10s"
}

