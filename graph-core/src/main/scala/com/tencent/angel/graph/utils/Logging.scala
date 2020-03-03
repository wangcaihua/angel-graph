package com.tencent.angel.graph.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = _

  // Method to get the logger name for this object
  protected lazy val logName: String = this.getClass.getName.stripSuffix("$")

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(isInterpreter = false)
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date)
    println(s"[$time] $msg")
  }

  protected def isTraceEnabled: Boolean = {
    log.isTraceEnabled
  }

  protected def initializeLogIfNecessary(isInterpreter: Boolean,
                                         silent: Boolean = false): Boolean = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter, silent)
          return true
        }
      }
    }
    false
  }

  private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    if (Logging.isLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        Logging.defaultSparkLog4jConfig = true
        val defaultLogProps = "com/tencent/angel/graph/log4j-defaults.properties"
        Option(getClass.getClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            if (!silent) {
              System.err.println(s"Using Angel-Graph's default log4j profile: $defaultLogProps")
            }
          case None =>
            System.err.println(s"Angel-Graph was unable to load $defaultLogProps")
        }
      }

      val rootLogger = LogManager.getRootLogger
      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val replLogger = LogManager.getLogger(logName)
        val replLevel = Option(replLogger.getLevel).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel) {
          if (!silent) {
            System.err.printf("Setting default log level to \"%s\".\n", replLevel)
            System.err.println("To adjust logging level use sc.setLogLevel(newLevel). ")
          }
          rootLogger.setLevel(replLevel)
        }
      }
      // scalastyle:on println
    }
    Logging.initialized = true

    log
  }
}

private[graph] object Logging {
  @volatile private var initialized = false
  @volatile private var defaultRootLevel: Level = _
  @volatile private var defaultSparkLog4jConfig = false

  val initLock = new Object()
  try {
    val bridgeClass = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }

  /**
    * Marks the logging system as not initialized. This does a best effort at resetting the
    * logging system to its initial state so that the next class to use logging triggers
    * initialization again.
    */
  def uninitialize(): Unit = initLock.synchronized {
    if (isLog4j12) {
      if (defaultSparkLog4jConfig) {
        defaultSparkLog4jConfig = false
        LogManager.resetConfiguration()
      } else {
        LogManager.getRootLogger.setLevel(defaultRootLevel)
      }
    }
    this.initialized = false
  }

  private def isLog4j12: Boolean = {
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
  }
}