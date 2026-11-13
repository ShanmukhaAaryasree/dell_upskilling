package com.capstone.fixture

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shared SparkSession test fixture. Creates ONE shared local embedded SparkSession for ALL test
  * classes.
  *
  * Java 25 compatibility: Calls HadoopConfig.init() before loading any Spark class, ensuring Hadoop
  * security properties are set before Subject.getSubject() is called.
  */
trait SparkSessionFixture extends BeforeAndAfterAll { this: Suite =>

  private object SparkSessionFixture {
    @volatile private var _spark: SparkSession = null
    private val lock = new Object()

    def getOrCreate(): SparkSession = {
      // Initialize Hadoop config BEFORE any Spark class is loaded
      HadoopConfig.init()

      if (_spark == null) {
        lock.synchronized {
          if (_spark == null) {
            val userName = System.getProperty("user.name")
            _spark = SparkSession
              .builder()
              .appName("CapstoneTest")
              .master("local[2]")
              .config("spark.sql.shuffle.partitions", "1")
              .config("spark.ui.showConsoleProgress", "false")
              .config("spark.log.level", "ERROR")
              .config("spark.driver.bindAddress", "127.0.0.1")
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.sql.adaptive.enabled", "false")
              .config("spark.sql.catalogImplementation", "in-memory")
              .config("spark.sql.streaming.forceApplyUpdatesToSink", "true")
              .config("spark.hadoop.fs.file.impl.disable.cache", "true")
              .config("spark.ui.enabled", "false")
              .config("spark.hadoop.hadoop.security.authentication", "simple")
              .config("spark.hadoop.HADOOP_USER_NAME", userName)
              .config("spark.driver.userName", userName)
              .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
              .getOrCreate()
            _spark.sparkContext.setLogLevel("ERROR")
          }
        }
      }
      _spark
    }
  }

  /** Returns the shared SparkSession. Safe to call from val initialization. */
  def buildSparkSession(): SparkSession = SparkSessionFixture.getOrCreate()
}
