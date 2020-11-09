package com.xl.flink.scala

import scala.util.Random

/**
 *
 * @author 夏龙
 * @date 2020-11-02
 */
object STJDB {
  def main(args: Array[String]): Unit = {

    var i: Int = 0
    for (i <- 1 to 10) {
      //玩家
      val a: Int = readLine().toInt
      //庄家
      val b = 1 + (new Random).nextInt((7 - 1) + 1)
      if (a >b) {
        print("玩家胜利！" + "\n")
      }
      else if (a ==b) {
        print("平局" + "\n")
      }
      else {
        print("庄家胜利" + "\n")
      }
    }
  }


}
