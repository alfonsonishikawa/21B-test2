package com.nishilua.test2

object TupleUdfs {

  import org.apache.spark.sql.functions.udf
  // type tag is required, as we have a generic udf
  import scala.reflect.runtime.universe.{TypeTag, typeTag}

  def toTuple2[S: TypeTag, T: TypeTag] =
    udf[(S, T), S, T]((x: S, y: T) => (x, y))

}
