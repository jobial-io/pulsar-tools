package io.jobial.pulsar.admin

import cats.Parallel
import cats.effect.IO
import cats.effect.Resource.make
import cats.implicits.catsSyntaxParallelSequence1
import org.apache.pulsar.client.admin.PulsarAdmin

import scala.collection.JavaConverters._

trait PulsarAdminUtils {

  def tenants(implicit admin: PulsarAdmin) =
    IO(admin.tenants().getTenants.asScala.toList.map(Tenant(_)))

  def namespaces(implicit admin: PulsarAdmin, parallel: Parallel[IO]) =
    for {
      tenants <- tenants
      namespaces <- tenants.map(_.namespaces).parSequence.map(_.flatten)
    } yield namespaces

  def topics(namespacePattern: String)(implicit admin: PulsarAdmin, parallel: Parallel[IO]) =
    for {
      namespaces <- namespaces
      topics <- {
        for {
          namespace <- namespaces if namespacePattern.r.matches(namespace.name)
        } yield namespace.topics
      }.parSequence.map(_.flatten)
    } yield topics

  def subscriptions(namespace: String)(implicit admin: PulsarAdmin, parallel: Parallel[IO]): IO[List[Subscription]] =
    for {
      topics <- topics(namespace)
      subscriptions <- subscriptions(topics)
    } yield subscriptions

  def consumers(namespace: String)(implicit admin: PulsarAdmin, parallel: Parallel[IO]) =
    for {
      topics <- topics(namespace)
      stats <- topics.map(_.stats).parSequence
      subStats = stats.map(_.getSubscriptions.asScala).flatten
      consumers = subStats.flatMap(_._2.getConsumers.asScala)
    } yield consumers

  def subscriptions(topics: List[Topic])(implicit admin: PulsarAdmin, parallel: Parallel[IO]): IO[List[Subscription]] =
    for {
      subscriptions <- topics.map(_.subscriptions).parSequence
    } yield {
      for {
        subscriptions <- subscriptions
        subscription <- subscriptions
      } yield subscription
    }.groupBy(_.name).toList.map { case (name, s) => Subscription(name, s.flatMap(_.topics)) }

}

case class Tenant(name: String) {

  def namespaces(implicit admin: PulsarAdmin) =
    IO(admin.namespaces().getNamespaces(name).asScala.toList.map(Namespace(_, this)))
}

case class Namespace(name: String, tenant: Tenant) {

  def topics(implicit admin: PulsarAdmin) =
    IO(admin.namespaces.getTopics(name).asScala.toList.map(Topic(_)))
}

case class Topic(name: String) {

  def subscriptions(implicit admin: PulsarAdmin): IO[List[Subscription]] =
    IO(admin.topics().getSubscriptions(name).asScala.toList.map(Subscription(_, List(this))))
  
  def stats(implicit admin: PulsarAdmin) =
    IO(admin.topics().getStats(name))
}

case class Subscription(name: String, topics: List[Topic]) {

//  def stats(implicit admin: PulsarAdmin) =
//    IO(admin.topics().getSt)

}

case class PulsarAdminContext(url: String = "http://localhost:8080", namespace: String = "public/default") {

  def admin = 
    make(IO(PulsarAdmin.builder.serviceHttpUrl(url).build))(admin => IO(admin.close))
}

