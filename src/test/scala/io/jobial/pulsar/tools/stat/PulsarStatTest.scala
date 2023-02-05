package io.jobial.pulsar.tools.stat

import cats.effect.IO
import cats.implicits.catsSyntaxFlatMapOps
import io.jobial.pulsar.tools.stat.PulsarStat.resolveHostname
import io.jobial.scase.core.test.ScaseTestHelper
import org.scalatest.flatspec.AsyncFlatSpec

import scala.language.postfixOps

class PulsarStatTest extends AsyncFlatSpec with ScaseTestHelper {

  "hostname" should "be resolved" in {
    IO(assert(resolveHostname("127.0.0.1").map(_.startsWith("localhost")).getOrElse(false)))
  }
}
