package fuse

import scalaz._

package object spark {

  implicit def DataTypeEqual: Equal[org.apache.spark.sql.types.DataType] =
    Equal.equalA
}
