package com.capstone.fixture

/** Static initializer that MUST be called before any Spark/Hadoop class is loaded. Sets all system
  * properties needed for Java 25 compatibility with Hadoop. This class is referenced by
  * SparkSessionFixture to ensure early initialization.
  */
object HadoopConfig {
  private val lock = new Object()
  private var _initialized = false

  def init(): Unit = {
    if (!_initialized) {
      lock.synchronized {
        if (!_initialized) {
          val userName = System.getProperty("user.name")

          // Set HADOOP_HOME to avoid winutils errors on Windows
          val hadoopHome = sys.env.get("HADOOP_HOME").getOrElse {
            val tmpHadoop = System.getProperty("java.io.tmpdir") + "/hadoop"
            new java.io.File(tmpHadoop).mkdirs()
            new java.io.File(tmpHadoop + "/bin").mkdirs()
            tmpHadoop
          }

          // MUST set these BEFORE any Spark/Hadoop class is loaded
          System.setProperty("hadoop.home.dir", hadoopHome)
          System.setProperty("hadoop.native.lib", "false")
          System.setProperty("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
          System.setProperty("hadoop.security.authentication", "simple")
          System.setProperty(
            "hadoop.security.group.mapping",
            "org.apache.hadoop.security.ShellBasedMapping"
          )
          System.setProperty("HADOOP_USER_NAME", userName)
          System.setProperty("HADOOP_GROUP_NAME", "users")
          System.setProperty("SPARK_USER", userName)

          _initialized = true
        }
      }
    }
  }
}
