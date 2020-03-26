package com.tencent.angel.graph.utils

class Profiler(val methodName: String) extends Logging {
  private var startTime: Long = System.currentTimeMillis()
  private var currentStepName: String = "not set"
  private var started: Boolean = false

  def start(stepName: String): Unit = {
    if ( this.started ) throw new Exception("profiler has already started")

    this.started = true
    this.startTime = System.currentTimeMillis()
    this.currentStepName = stepName

    logTime(s"profiling ${this.methodName}: '${this.currentStepName}' started")
  }

  def stop(): Unit = {
    if ( !this.started ) throw new Exception("profiler has not yet started")

    this.started = false
    val endTime = System.currentTimeMillis()
    val duration = endTime - this.startTime

    logTime(s"profiling ${this.methodName}: '${this.currentStepName}' finished, cost ${duration}ms")
  }
}
