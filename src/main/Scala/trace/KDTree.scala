//package trace
//
///**
// * @Time：2021/5/19
// * @Author：JiQT
// * @File：KDTree
// * @Software：IDEA
// **/
//
///**
// * KD树算法.
// */
//object KDTree {
//
//  /**
//   *
//   * @param value 节点数据
//   * @param dim   当前切分维度
//   * @param left  左子结点
//   * @param right 右子结点
//   */
//  case class TreeNode(value: Seq[Double],
//                      dim: Int,
//                      var left: TreeNode,
//                      var right: TreeNode) {
//
//    var parent: TreeNode = _ //父结点
//    var brotherNode: TreeNode = _ //兄弟结点
//
//    if (left != null) {
//      left.parent = this
//      left.brotherNode = right
//    }
//
//    if (right != null) {
//      right.parent = this
//      right.brotherNode = left
//    }
//
//  }
//
//  /**
//   *
//   * @param value 数据序列
//   * @param dim   当前划分的维度
//   * @param shape 数据维数
//   * @return
//   */
//  def creatKdTree(value: Seq[Seq[Double]], dim: Int, shape: Int): TreeNode = {
//
//    // 数据按照当前划分的维度排序
//    val sorted = value.sortBy(_ (dim))
//    //中间位置的索引
//    val midIndex: Int = value.length / 2
//
//    sorted match {
//      // 当节点为空时，返回null
//      case Nil => null
//      //节点不为空时，递归调用
//      case _ =>
//        val left = sorted.slice(0, midIndex)
//        val right = sorted.slice(midIndex + 1, value.length)
//
//        val leftNode = creatKdTree(left, (dim + 1) % shape, shape) //左子节点递归创建树
//        val rightNode = creatKdTree(right, (dim + 1) % shape, shape) //右子节点递归创建树
//        TreeNode(sorted(midIndex), dim, leftNode, rightNode)
//    }
//  }
//
//
//  //坐标距离算法
//  def vectorDis(v1: Seq[Double], v2: Seq[Double]): Double = {
//    val lng1: Double = v1(0)
//    val lat1: Double = v1(1)
//    val lng2: Double = v2(0)
//    val lat2: Double = v2(1)
//    val radLat1: Double = lat1 * Math.PI / 180.0
//    val radLng1: Double = lng1 * Math.PI / 180.0
//    val radLat2: Double = lat2 * Math.PI / 180.0
//    val radLng2: Double = lng2 * Math.PI / 180.0
//    6378137.0 * Math.acos(Math.cos(radLat1) * Math.cos(radLat2) * Math.cos(radLng1 - radLng2) + Math.sin(radLat1) * Math.sin(radLat2))
//  }
//  // 欧式距离算法
//    def euclidean(p1: Seq[Double], p2: Seq[Double]) = {
//    require(p1.size == p2.size)
//    val d = p1
//      .zip(p2)
//      .map(tp => math.pow(tp._1 - tp._2, 2))
//      .sum
//    math.sqrt(d)
//  }
//
//  /**
//   *
//   * @param treeNode kdtree
//   * @param data     查询点
//   *                 最近邻搜索
//   */
//  def nearestSearch(treeNode: TreeNode, data: Seq[Double],ePs: Double, minPts: Int): TreeNode = {
//
//    var nearestNode: TreeNode = null //当前最近节点
//    var minDist: Double = ePs //当前最小距离
//
//    def finder(treeNode: TreeNode): TreeNode = {
//      treeNode match {
//        case null => nearestNode
//        case _ =>
//          val dimr = data(treeNode.dim) - treeNode.value(treeNode.dim)
//          if (dimr > 0) finder(treeNode.right) else finder(treeNode.left)
//
//          val distc = vectorDis(treeNode.value, data)
//          if (distc <= minDist) {
//            minDist = distc
//            nearestNode = treeNode
//          }
//
//          // 目标点与当前节点相交
//          if (math.abs(dimr) < minDist)
//            if (dimr > 0) finder(treeNode.left) else finder(treeNode.right)
//          nearestNode
//      }
//    }
//    finder(treeNode)
//  }
//}