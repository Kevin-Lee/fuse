package fuse

import org.apache.spark.sql.{Column, DataFrame, Row}
import cats._
import cats.data._
import cats.syntax.all._
import cats.instances.all._

/**
 * Support selecting columns from a `DataFrame` in a composable/safe API.
 *
 * It's expected to be used in a [Applicative](http://eed3si9n.com/learning-scalaz/Applicative.html)
 * style, where each column select can be "applied" separately.
 *
 * NOTE: By design can't be a `Monad`, which unfortunately means we can't use `for` comprehensions to compose.
 *
 * For concrete examples please look at the `SelectSpec`.
 */
case class Select[A](
    columns: List[Column]
  , parseRow: RowParser[A]
  ) {

  def map[B](f: A => B): Select[B] =
    Select(columns, parseRow.map(f))

  /** Only select the head from the `DataFrame`, which will be common for aggregate column usage */
  def head(data: DataFrame): Either[RowParserError, A] =
    parseRow(data.select(columns: _*).head)

  type RowParserErrorOrList[A] = Either[RowParserError, A]

  def toList(data: DataFrame): Either[RowParserError, List[A]] =
    data.select(columns: _*).collect.toList
      .traverse(row => parseRow(row): RowParserErrorOrList[A])
}

object Select {

  implicit def SelectApplicative: Applicative[Select] =
    new Applicative[Select] {

      override def pure[A](a: A): Select[A] =
        constant(a)

      override def ap[A, B](f: Select[A => B])(fa: Select[A]): Select[B] =
        Select(
          fa.columns ++ f.columns
        , fa.parseRow.flatMap(j => f.parseRow.map(k => k(j)))
        )
    }

  def column[A](c: Column): Select[A] =
    Select(List(c), RowParser.getAs[A])

  def constant[A](a: => A): Select[A] =
    Select(Nil, RowParser.point(a))
}

