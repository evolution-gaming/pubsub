package akka.cluster.pubsub

import scala.reflect.ClassTag

object CopyArray {
  def apply[T: ClassTag](from: Array[T]): Array[T] = {
    val length = from.length
    val to = new Array[T](length)
    System.arraycopy(from, 0, to, 0, length)
    to
  }
}