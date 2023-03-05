package io.jobial.pulsar.tools.listen

import cats.effect.IO
import cats.instances.uuid
import io.jobial.scase.core.MessageHandler
import io.jobial.scase.marshalling.Unmarshaller
import io.jobial.scase.marshalling.rawbytes._
import io.jobial.scase.pulsar.PulsarContext
import io.jobial.scase.pulsar.PulsarServiceConfiguration.handler
import io.jobial.sclap.CommandLineApp
import org.apache.pulsar.client.api.Message

import java.time.Instant.ofEpochMilli
import scala.io.AnsiColor._
import scala.util.Try

object PulsarListen extends CommandLineApp {

  def run =
    for {
      host <- opt[String]("host", "H").default(PulsarListenContext().host)
      port <- opt[Int]("port", "p").default(PulsarListenContext().port)
      messageSizeLimit <- opt[Int]("message-size-limit", "s").
        description("Truncate message above this size")
      topicPattern <- opt[String]("topic", "t").default("public/default/.*")
      context = PulsarListenContext(host, port, topicPattern, messageSizeLimit)
      r <- run(context)
    } yield r

  def run(implicit context: PulsarListenContext) = command {
    for {
      config <- IO(handler[Array[Byte]](context.topicPattern.r, None, None, s"pulsar-listen-${uuid}"))
      service <- {
        implicit val pulsarContext = context.pulsarContext
        config.service(messageHandler)
      }
      _ <- service.startAndJoin
    } yield ()
  }

  val tibrvUnmarshaller = Try(Class.forName("io.jobial.pulsar.tools.listen.TibrvMsgUnmarshaller")
    .getDeclaredConstructor().newInstance().asInstanceOf[Unmarshaller[String]]).toEither

  def messageHandler(implicit context: PulsarListenContext) = MessageHandler[IO, Array[Byte]](implicit messageContext => { message =>
    for {
      pulsarMessage <- messageContext.receiveResult().underlyingMessage[Message[_]]
      _ <- IO {
        val result = tibrvUnmarshaller.flatMap(_.unmarshal(message))
          .getOrElse(Try(new String(message, "UTF-8").replaceAll("\\P{Print}", ".")).toEither).toString
        val messageSizeLimit = context.messageSizeLimit.getOrElse(Int.MaxValue) 
        println(s"${YELLOW}${ofEpochMilli(pulsarMessage.getPublishTime)} ${GREEN}${pulsarMessage.getTopicName}${RESET} ${result.take(messageSizeLimit)}${if (result.size > messageSizeLimit) "..." else ""}")
      }
    } yield ()
  })
}

case class PulsarListenContext(host: String = "localhost", port: Int = 6650, topicPattern: String = ".*",
  messageSizeLimit: Option[Int] = None) {

  def pulsarContext = PulsarContext(host, port)
}