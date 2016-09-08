import java.util.Properties

import Model.{BTCTicker, SpreadEvt, Ticker, TradeEvt}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.api.scala._
import Const._
import akka.actor.Status.Success
import com.paulgoldbaum.influxdbclient.Parameter.Precision
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import com.paulgoldbaum.influxdbclient._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try

/**
  * Created by M.Sumner on 20/06/2016.
  */
object FlinkConsumer extends App {

  val (env, properties, influxdb, tradeStream, spreadStream, bitstampPollStream, cmcPollStream) = init

  /** Publish values to InfluxDB ****/
  publishRawData

  /** Pattern 1: Match all executions under a threshold ****/
  val threshold = 680
  val priceCheck = Pattern.begin[TradeEvt]("start")
    .where(_.price <= threshold)

  val priceCheckStream = CEP.pattern(tradeStream, priceCheck)

  priceCheckStream.select(x => {
    x.get("start").map(x => {
      val data = Point("p_exec_threshold")
        .addTag("event-time", x.timestamp)
        .addField("price", x.price)
        .addField("side", x.side)

      write(data)

      x
    })
  })

  /** Pattern 2: Alert on price fluctuation of x dollars in 10m periods ****/
  val fluctuationThreshold = 3.0
  val consecutiveMatch =
    Pattern.begin[TradeEvt]("start")
        .where(_.side == 0)
      .followedBy("end")
        .where(_.side == 0)
      .within(Time.minutes(10))

  val consecutiveMatchStream = CEP.pattern(tradeStream, consecutiveMatch)

  val priceDelta = consecutiveMatchStream.select(x => {
      val a = x.get("start")
      val b = x.get("end")

      for (
        p1 <- a;
        p2 <- b;
        diff = p2.price - p1.price
        if (Math.abs(diff) > fluctuationThreshold)
      ) yield {
          val data = Point("p_fluctuation")
            .addTag("event-time", p1.timestamp)
            .addField("diff", diff)
        write(data)

        s"Price change: ${diff}\nStart: ${p1}\nEnd: ${p2}\n"
      }
  } getOrElse(""))

  priceDelta.print

  /** Pattern 3: Alert when we have two polling events with price over VWAP (last 24hr) within a period x ****/
  val vwapMatchOver =
    Pattern.begin[BTCTicker]("start")
      .where(x => x.last.toDouble >= x.vwap.toDouble)
      .next("end")
      .where(y => y.last.toDouble >= y.vwap.toDouble)
      .within(Time.minutes(1))

  val vwapMatchStreamOver = CEP.pattern(bitstampPollStream, vwapMatchOver)

  val diff = vwapMatchStreamOver.select(x => s"Over VWAP by: ${
    x.get("start").map(x => {
      val diff = calcPercentageDiff(x.last.toDouble, x.vwap.toDouble)

      val data = Point("p_overvwap")
        .addTag("event-time", x.timestamp)
        .addField("diff", diff)
      write(data)

      diff
    }) getOrElse("")}%")

  diff.print

  env.execute("Consumer")


  def init = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val tradeStream = env
      .addSource(new FlinkKafkaConsumer09[TradeEvt](tradeTop, new AvroDeserializationSchema[TradeEvt](classOf[TradeEvt]), properties))

    val spreadStream = env
      .addSource(new FlinkKafkaConsumer09[SpreadEvt](spreadTop, new AvroDeserializationSchema[SpreadEvt](classOf[SpreadEvt]), properties))

    val bitstampPollStream = env
      .addSource(new FlinkKafkaConsumer09[BTCTicker](bitstampTickerTop, new AvroDeserializationSchema[BTCTicker](classOf[BTCTicker]), properties))

    val cmcPollStream = env
      .addSource(new FlinkKafkaConsumer09[Ticker](tickerTop, new AvroDeserializationSchema[Ticker](classOf[Ticker]), properties))

    val influxdb = InfluxDB.connect("localhost", 8086)

    (env, properties, influxdb, tradeStream, spreadStream, bitstampPollStream, cmcPollStream)
  }

  def calcPercentageDiff(a: Double, b: Double) = (a - b) / b * 100

  def write(data: Point) = {
    val database = influxdb.selectDatabase("test")
    database.write(data)
  }

  def publishRawData = {
    tradeStream.map(x => {
      val data = Point("executions")
        .addTag("id", x.id.toString)
        .addField("price", x.price)
        .addField("side", x.side)
        .addField("qty", x.amount)
        .addField("event-time", x.timestamp)

      write(data)
    })

    spreadStream.map(x => {
      val data = Point("spreads")
        .addTag("event-time", x.timestamp)
        .addField("bids", x.bids.flatten.foldLeft("")((acc, x) => acc + s"${x}\n"))
        .addField("asks", x.asks.flatten.foldLeft("")((acc, x) => acc + s"${x}\n"))

      write(data)
    })

    bitstampPollStream.map(x => {
      val data = Point("btc")
        .addTag("event-time", x.timestamp)
        .addField("price", x.last.toDouble)
        .addField("vwap", x.vwap.toDouble)
        .addField("high-ask", x.ask.toDouble)
        .addField("high-bid", x.bid.toDouble)
        .addField("high", x.high.toDouble)
        .addField("low", x.low.toDouble)
        .addField("vol", x.volume.toDouble)

      write(data)
    })

    cmcPollStream.map( x => {
        val data = Point("cmc")
          .addTag("symbol", x.symbol.toString)
          .addField("price_usd", x.price_usd)
          .addField("marketcap", x.marketCap)
          .addField("rank", x.rank)
          .addField("supply", x.totalSupply)
          .addField("day-vol", x.dayVol)
          .addField("delta-week", x.deltaHour)
          .addField("delta-day", x.deltaDay)
          .addField("delta-week", x.deltaWeek)

        write(data)
      })
  }

}
