package io.jobial.pulsar.tools.top

import cats.effect.IO
import com.googlecode.lanterna.TextColor
import com.googlecode.lanterna.TextColor.ANSI._
import com.googlecode.lanterna.screen.Screen
import com.googlecode.lanterna.screen.TerminalScreen
import com.googlecode.lanterna.terminal.DefaultTerminalFactory
import io.jobial.pulsar.admin.PulsarAdminContext
import io.jobial.pulsar.admin.PulsarAdminUtils
import io.jobial.sclap.CommandLineApp

import scala.math.min

object PulsarTop extends CommandLineApp with PulsarAdminUtils {

  def run = command.printStackTraceOnException(true) {
    for {
      screen <- IO {
        val term = new DefaultTerminalFactory().createTerminal
        implicit val screen = new TerminalScreen(term)
        screen.startScreen
        screen
      }
      _ <- printHeader(screen)
      _ <- {
        for (i <- 1 until 10)
          yield {
            val r = Row(List(Cell("Hello", 10), Cell("World", 10), Cell("Hello", 10), Cell("World", 10), Cell("Hello", 10), Cell("World", 10)))
            r.print(0, i)(screen)
          }
      }.toList.sequence
      _ <- IO(screen.setCursorPosition(null))
      _ <- IO(screen.refresh())
    } yield ()
  }

  def printHeader(implicit screen: Screen) = {
    val background = screen.newTextGraphics.getBackgroundColor
    val c = Cell("", 10, foregroundColor = Some(background), backgroundColor = Some(GREEN))
    val r = Row(List(
      c.copy("Topic", 10),
      c.copy("MsgRateIn", 10),
      c.copy("MsgRateOut", 10),
      c.copy("TputInMB", 10),
      c.copy("TputOutMB", 10)
    ))
    r.print(0, 0)
  }

  def printRows()(implicit screen: Screen, context: PulsarAdminContext) =
    for {
      _ <- topics
        _ <- printHeader

    }
}

case class Row(cells: List[Cell]) {

  def print(column: Int, row: Int)(implicit screen: Screen): IO[Unit] =
    cells match {
      case Nil => IO()
      case cell :: _ =>
        for {
          _ <- cell.print(column, row)
          _ <- Row(cells.tail).print(column + cell.width, row)
        } yield ()
    }

}

case class Cell(content: String, width: Int, foregroundColor: Option[TextColor] = Some(GREEN), backgroundColor: Option[TextColor] = None) {

  def print(column: Int, row: Int)(implicit screen: Screen) = IO {
    val textGraphics = screen.newTextGraphics
    foregroundColor.map(textGraphics.setForegroundColor)
    backgroundColor.map(textGraphics.setBackgroundColor)
    textGraphics.putString(column, row, content.substring(0, min(content.length, width)).padTo(width, ' '))
  }

}