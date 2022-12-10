package dbscan

import scala.math.cos

/**
 * 左下角(x, y),右上角(x2, y2)
 */
case class DBSCANRectangle(x: Double, y: Double, x2: Double, y2: Double) {

  /**
   * 是否包含在矩形中（矩形）
   */
  def contains(other: DBSCANRectangle): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }

  /**
   * 是否包含在矩形中包括边界上的点（点）
   */
  def contains(point: DBSCANPoint): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2
  }

  /**
   * 通过将此框缩小给定的量返回一个新框
   */
  def shrink(amount: Double): DBSCANRectangle = {
    DBSCANRectangle(x + amount, y + amount, x2 - amount, y2 - amount)
  }
  def shrinkGeo(amount: Double):DBSCANRectangle={
    val d = amount / (6378137.0 * 1000)
    val cx=cos(y* Math.PI / 180.0)
    val x1= d / cx * 180.0 / Math.PI+x
    val y1= d / Math.PI * 180.0+y
    DBSCANRectangle(x, y, x1, y1)
    DBSCANRectangle(x + amount, y + amount, x2 - amount, y2 - amount)
  }

  /**
   * 是否包含在矩形中，不含边界（点）
   */
  def almostContains(point: DBSCANPoint): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2
  }

}
