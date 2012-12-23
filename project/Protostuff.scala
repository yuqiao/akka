package akka

import sbt._
import sbt.Classpaths
import sbt.FileFunction
import sbt.FilesInfo
import sbt.IO
import sbt.Keys._
import sbt.Logger
import sbt.Process
import sbt.Project.Initialize
import sbt.SettingKey
import sbt.TaskKey
import java.io.File
import com.dyuproject.protostuff.compiler.{CompilerMain, ProtoModule}

object Protostuff {
  val protostuffConfig = config("protobuf")

  val generateSources = TaskKey[Seq[File]]("generate-sources", "Compile the protobuf sources.")

  lazy val protostuffSettings: Seq[Setting[_]] = inConfig(protostuffConfig)(Seq[Setting[_]](
    sourceDirectory <<= (sourceDirectory in Compile) { _ / "protobuf" },
    javaSource <<= (sourceManaged in Compile) { _ / "protostuff_sources" },
    version := "1.0.7",
    generateSources <<= sourceGeneratorTask

  )) ++ Seq[Setting[_]](
    sourceGenerators in Compile <+= (generateSources in protostuffConfig).identity,
    managedSourceDirectories in Compile <+= (javaSource in protostuffConfig).identity,
    cleanFiles <+= (javaSource in protostuffConfig).identity,
    libraryDependencies <+= (version in protostuffConfig)("com.dyuproject.protostuff" % "protostuff-core" % _),
    ivyConfigurations += protostuffConfig
  )

  private def compile(srcFile: File, targetDir: File): Unit = {
    val module = new ProtoModule(srcFile, "java_bean", "UTF-8", targetDir)
    module.setOption("builder_pattern", "true")
    module.setOption("generate_helper_methods", "true")
    CompilerMain.compile(module)
  }

  private def compileDir(srcDir: File, targetDir: File, log: Logger) = {
    val schemas = (srcDir ** "*.proto").get
    targetDir.mkdirs()
    log.info("Compiling %d protobuf files to %s".format(schemas.size, target))
    schemas.foreach {
      schema =>
        log.info("Compiling schema %s" format schema)
        compile(schema, targetDir)
    }

    (targetDir ** "*.java").get.toSet
  }

  private def sourceGeneratorTask = (sourceDirectory in protostuffConfig, javaSource in protostuffConfig, cacheDirectory, streams) map {
    (srcDir, targetDir, cache, s) =>
      val cachedCompile = FileFunction.cached(cache / "protobuf", inStyle = FilesInfo.lastModified, outStyle = FilesInfo.exists) { (in: Set[File]) =>
        compileDir(srcDir, targetDir, s.log)
      }
      cachedCompile((srcDir ** "*.proto").get.toSet).toSeq
  }

}
