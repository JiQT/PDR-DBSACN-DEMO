package dbscan

import org.apache.spark.mllib.linalg.Vector

import java.lang.Math.sqrt

case class DBSCANPoint(val vector: Vector) {

  def x = vector(0)
  def y = vector(1)

  def distanceSquared1(other: DBSCANPoint)={
    val dx = other.x - x
    val dy = other.y - y
    var dis=sqrt((dx * dx) + (dy * dy))
    dis
  }
  def distanceSquared(other: DBSCANPoint): Double = {

    val radLat1: Double = x * 3.14159 / 180.0
    val radLng1: Double = y * 3.14159 / 180.0
    val radLat2: Double = other.x * 3.14159 / 180.0
    val radLng2: Double = other.y * 3.14159 / 180.0
    6378137.0* Math.acos(Math.cos(radLat1) * Math.cos(radLat2) * Math.cos(radLng1 - radLng2) + Math.sin(radLat1) * Math.sin(radLat2))
  }

}
