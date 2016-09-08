import java.io.{ByteArrayOutputStream, FileInputStream}
import java.util.Properties

import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import spray.json.JsonFormat

import scala.util.{Failure, Success, Try}

import spray.json.{JsonFormat, _}
import DefaultJsonProtocol._

/**
  * Created by M.Sumner on 20/06/2016.
  */
object Util {

  def parse[T](data: String)(implicit format : JsonFormat[T]) = {
    Try {
      val parsed = data.parseJson
      parsed.convertTo[T]
    }
  }

  def parseCollection[T](data: String)(implicit format : JsonFormat[T]) = {
    Try {
      val parsed = data.parseJson
      parsed.convertTo[Seq[T]]
    }
  }

  def serialize[T](obj: T) = {
    val schema = ReflectData.get().getSchema(obj.getClass)
    val writer = new ReflectDatumWriter[T](schema)
    val out = new ByteArrayOutputStream

    Try {
      writer.write(obj, EncoderFactory.get.directBinaryEncoder(out, null))
      out.toByteArray
    }
  }

  def getFileLines(fileName : String) = scala.io.Source.fromFile(fileName).mkString.split("\n")

  def loadPropertiesFile(fileName : String) = {
    val prop = new Properties()

    Try{
      prop.load(new FileInputStream(fileName))
    } match {
      case Failure(e) => e.printStackTrace; System.exit(1)
      case Success(msg) => ()
    }

    prop

  }

}
