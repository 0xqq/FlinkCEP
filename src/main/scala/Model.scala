import com.pusher.client.channel.SubscriptionEventListener
import com.pusher.client.connection.{ConnectionEventListener, ConnectionStateChange}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.{Failure, Success}
import Util._
import spray.json.JsonFormat

/**
  * Created by M.Sumner on 20/06/2016.
  */
object Model {

  trait ParsedType { def desugar: OutputType }
  trait OutputType

  /* Real time events */
  case class TradeEvt(price: Double, timestamp: String, amount: Double, side: Short, id: Long) extends ParsedType with OutputType {
    def this() = this(0.0, "", 0.0, 0, 0L)

    override def toString = s"price: ${price}, qty: $amount, side: $side, timestamp: ${timestamp}, id: $id"

    override def desugar = this
  }

  case class SpreadEvt(timestamp: String, bids: Array[Array[String]], asks: Array[Array[String]]) extends ParsedType with OutputType {
    def this() = this("", Array.empty, Array.empty)

    override def toString = s"timestamp: $timestamp, bids: ${bids.toList.flatten}, asks: ${asks.toList.flatten}"

    override def desugar = this
  }


  /* Polling events */
  case class TickerEvt(id: String, name: String, symbol: String, rank: Int, price_usd: Double,
                       dayVol: Option[Long], marketCap: Option[Long], availableSupply: Option[Long], totalSupply: Option[Long],
                       deltaHour: Option[Double], deltaDay: Option[Double], deltaWeek: Option[Double]) extends ParsedType {
    def this() = this("", "", "", 0, 0.0, Some(0L), Some(0L), Some(0L), Some(0L), Some(0.0), Some(0.0), Some(0.0))

    override def desugar = Ticker(
      id = id, name = name, symbol = symbol, rank = rank, price_usd = price_usd,
      dayVol = dayVol.getOrElse(0L), marketCap = marketCap.getOrElse(0L), availableSupply = availableSupply.getOrElse(0L),
      totalSupply = totalSupply.getOrElse(0L), deltaHour = deltaHour.getOrElse(0.0), deltaDay = deltaDay.getOrElse(0.0), deltaWeek = deltaWeek.getOrElse(0.0))
  }

  case class Ticker(id: String, name: String, symbol: String, rank: Int, price_usd: Double,
                    dayVol: Long, marketCap: Long, availableSupply: Long, totalSupply: Long,
                    deltaHour: Double, deltaDay: Double, deltaWeek: Double) extends OutputType {
    def this() = this("", "", "", 0, 0.0, 0L, 0L, 0L, 0L, 0.0, 0.0, 0.0)

    override def toString = s"name: $name, symbol: $symbol, rank: $rank, price: $price_usd"
  }

  case class BTCTicker(high: String, last: String, timestamp: String, bid: String, vwap: String, volume: String, low: String, ask: String, open: String) extends ParsedType with OutputType {
    def this() = this("0.0", "0.0", "", "0.0", "0.0", "0.0", "0.0", "0.0", "0.0")

    override def desugar = this
  }

//  {"high": "648.00000", "last": "647.99000", "timestamp": "1467281188", "bid": "645.94000", "vwap": "633.12714", "volume": "7873.57707267", "low": "626.71000", "ask": "647.99000", "open": "636.67000"}

  /* Event handling */
  object connListener extends ConnectionEventListener {
    override def onConnectionStateChange(change: ConnectionStateChange) =
      println(s"State changed to ${change.getCurrentState()} from ${change.getPreviousState()}")

    override def onError(message: String, code: String, e: Exception) =
      println("There was a problem connecting!")
  }

  case class myEventListener[T <: ParsedType](producer: KafkaProducer[String, Array[Byte]], topic: String)(format: JsonFormat[T]) extends SubscriptionEventListener {
    def poll(url: String)(isCollection: Boolean) = {
      val data = scala.io.Source.fromURL(url).mkString
      println("Received event with data: " + data)

      if(isCollection)
        parseCollection[T](data)(format) match {
        case Failure(e) => e.printStackTrace()
        case Success(parsedData) => sendSeq(parsedData)
      }
      else
        parse[T](data)(format) match {
        case Failure(e) => e.printStackTrace()
        case Success(parsedData) => send(parsedData)
      }
    }

    override def onEvent(channel: String, event: String, data: String) {
      println("Received event with data: " + data)

      parse[T](data)(format) match {
        case Failure(e) => e.printStackTrace()
        case Success(parsedData) => send(parsedData)
      }
    }

    def send(data: ParsedType) = serialize(data.desugar).map(arr => producer.send(new ProducerRecord[String, Array[Byte]](topic, arr)))

    def sendSeq(data: Seq[ParsedType]) = data.map(x => serialize(x.desugar).map(arr => producer.send(new ProducerRecord[String, Array[Byte]](topic, arr))))
  }

}
