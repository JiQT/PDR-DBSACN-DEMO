package dbscan

import scala.annotation.tailrec

/**
 * Top level method for creating a DBSCANGraph
 */
object DBSCANGraph {

  /**
   * 创建一个有向无权图
   */
  def apply[T](): DBSCANGraph[T] = new DBSCANGraph(Map[T, Set[T]]())

}

/**
 * An immutable unweighted graph with vertexes and edges
 * 具有顶点和边的不可变无权图
 */
class DBSCANGraph[T] private (nodes: Map[T, Set[T]]) extends Serializable {

  /**
   * Add the given vertex `v` to the graph
   *
   */
  def addVertex(v: T): DBSCANGraph[T] = {
    nodes.get(v) match {
      case None    => new DBSCANGraph(nodes + (v -> Set()))
      case Some(_) => this
    }
  }

  /**
   * Insert an edge from `from` to `to`
   */
  def insertEdge(from: T, to: T): DBSCANGraph[T] = {
    nodes.get(from) match {
      case None       => new DBSCANGraph(nodes + (from -> Set(to)))
      case Some(edge) => new DBSCANGraph(nodes + (from -> (edge + to)))
    }
  }

  /**
   * Insert a vertex from `one` to `another`, and from `another` to `one`
   *
   */
  def connect(one: T, another: T): DBSCANGraph[T] = {
    insertEdge(one, another).insertEdge(another, one)
  }

  /**找到所有包含from边的顶点vertexes
   * Find all vertexes that are reachable from `from`
   */
  def getConnected(from: T): Set[T] = {
    getAdjacent(Set(from), Set[T](), Set[T]()) - from
  }

  @tailrec
  private def getAdjacent(tovisit: Set[T], visited: Set[T], adjacent: Set[T]): Set[T] = {
    tovisit.headOption match {
      case Some(current) =>
        nodes.get(current) match {
          case Some(edges) =>
            // a.diff(b):返回a中不包含b的部分
            getAdjacent(edges.diff(visited) ++ tovisit.tail, visited + current, adjacent ++ edges)
          case None => getAdjacent(tovisit.tail, visited, adjacent)
        }
      case None => adjacent
    }
  }
}
