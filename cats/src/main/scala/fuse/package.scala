package fuse

import cats._

package object spark {

  implicit def DataTypeEqual: Eq[org.apache.spark.sql.types.DataType] =
    Eq.fromUniversalEquals
}
