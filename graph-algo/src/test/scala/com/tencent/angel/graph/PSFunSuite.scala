package com.tencent.angel.graph


import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Outcome}

trait PSFunSuite extends AnyFunSuite with BeforeAndAfterAll {

  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    try {
      println(s"\n\n===== TEST OUTPUT FOR $suiteName: '$testName' ======\n")
      test()
    } finally {
      println(s"\n===== FINISHED $suiteName: '$testName' ======\n")
    }
  }

}