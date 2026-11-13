package com.training

import java.nio.file.{Files, Path, Paths}
import java.util.Locale

object WindowsHadoopSupport {
  private val bundledHadoopVersion = "3.5.0"

  def configure(): Unit = {
    val isWindows = System.getProperty("os.name", "")
      .toLowerCase(Locale.ROOT)
      .contains("win")

    if (isWindows) {
      val candidates = Seq(
        sys.env.get("HADOOP_HOME"),
        Option(System.getProperty("hadoop.home.dir")),
        Some(Paths.get("target", s"hadoop-$bundledHadoopVersion").toAbsolutePath.toString)
      ).flatten.filter(_.nonEmpty)

      candidates
        .map(Paths.get(_))
        .find(home => Files.isRegularFile(home.resolve("bin").resolve("winutils.exe")))
        .foreach(home => System.setProperty("hadoop.home.dir", home.toAbsolutePath.toString))
    }
  }
}
