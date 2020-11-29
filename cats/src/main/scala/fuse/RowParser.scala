package fuse

import cats._
import cats.syntax.all._
import org.apache.spark.sql.Row

trait RowParser[A] { self =>

  def parse(index: Int, row: Row): Either[RowParserError, (Int, A)]

  final def apply(row: Row): Either[RowParserError, A] =
    parse(0, row).map(_._2)

  def map[B](f: A => B): RowParser[B] =
    new RowParser[B] {
      def parse(index: Int, row: Row): Either[RowParserError, (Int, B)] =
        self.parse(index, row).map {
          case ((index, a)) => (index, f(a))
        }
    }

  def flatMap[B](f: A => RowParser[B]): RowParser[B] =
    new RowParser[B] {
      def parse(index: Int, row: Row): Either[RowParserError, (Int, B)] =
        self.parse(index, row) match {
          case Left(e) =>
            e.asLeft
          case Right((i, a)) =>
            f(a).parse(i, row)
        }
    }

  def flatMapFail[B](f: A => Either[RowParserError, B]): RowParser[B] =
    self.flatMap(r => f(r).fold(RowParser.fail, RowParser.point(_)))

}

object RowParser {

  implicit def RowParserMonad: Monad[RowParser] =
    new Monad[RowParser] {

      override def pure[A](a: A): RowParser[A] =
        RowParser.point(a)

      override def flatMap[A, B](fa: RowParser[A])(f: A => RowParser[B]): RowParser[B] =
        fa.flatMap(f)

      /**
       * It is not stack safe.
       *
       * @param a
       * @param f
       * @tparam A
       * @tparam B
       * @return
       */
      @deprecated("It is not stack safe so do not use it.", since = "The beginning")
      override def tailRecM[A, B](a: A)(f: A => RowParser[Either[A, B]]): RowParser[B] = {
        def tailRec(result: Either[A, B]): RowParser[B] =
          result match {
            case Right(b) =>
              RowParser.point(b)
            case Left(next) =>
              f(next).flatMap(
                _.fold(
                  nextA => tailRec(nextA.asLeft),
                  b => tailRec(b.asRight)
                )
              )
          }
        f(a).flatMap(tailRec)
      }
    }

  def fail[A](e: RowParserError): RowParser[A] =
    new RowParser[A] {
      def parse(_i: Int, _row: Row): Either[RowParserError, (Int, A)] =
        e.asLeft
    }

  def point[A](a: => A): RowParser[A] =
    new RowParser[A] {
      def parse(i: Int, _row: Row): Either[RowParserError, (Int, A)] =
        (i, a).asRight
    }

  def row: RowParser[Row] =
    new RowParser[Row] {
      def parse(i: Int, row: Row): Either[RowParserError, (Int, Row)] =
        (i, row).asRight
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  def getAs[A]: RowParser[A] =
    new RowParser[A] {
      def parse(i: Int, row: Row): Either[RowParserError, (Int, A)] =
        if (i < 0 || i >= row.length) {
          RowParserError.indexOutOfBounds(i).asLeft
        } else {
          try {
            val a = row.get(i)
            (i + 1, a.asInstanceOf[A]).asRight
          } catch {
            case _: ClassCastException =>
              // FIXME This doesn't appear to actually work, will need some investigation
              RowParserError.classCast(i).asLeft
          }
        }
    }

  def getAsOption[A]: RowParser[Option[A]] =
    getAs[A].map(Option(_))
}

sealed trait RowParserError {

  def render: String =
    this match {
      case RowParserError.ClassCast(i) =>
        s"Invalid column type parsed at column $i"
      case RowParserError.IndexOutOfBounds(i) =>
        s"Invalid column index for column $i"
      case RowParserError.InvalidValue(i, message) =>
        s"Invalid value at column $i - $message"
    }
}

object RowParserError {

  case class ClassCast(i: Int) extends RowParserError
  case class IndexOutOfBounds(i: Int) extends RowParserError
  case class InvalidValue(i: Int, message: String) extends RowParserError

  def classCast(i: Int): RowParserError =
    ClassCast(i)

  def indexOutOfBounds(i: Int): RowParserError =
    IndexOutOfBounds(i)

  def invalidValue(i: Int, message: String): RowParserError =
    InvalidValue(i, message)
}
