import com.pusher.client.Pusher
import com.pusher.client.connection.ConnectionState
import org.apache.kafka.clients.producer.KafkaProducer
import Model._
import Const._

/**
  * Created by M.Sumner on 20/06/2016.
  */
object EventGenerator extends App {

  val (pusher, producer) = init

  /* Connect and subscribe to channels */
  pusher.connect(connListener, ConnectionState.ALL)
  val tradeSub = pusher.subscribe(Const.tradeCh)
  val spreadSub = pusher.subscribe(Const.spreadCh)

  /* Bind to real time events */
  tradeSub.bind(tradeEvt, myEventListener[TradeEvt](producer, tradeTop)(tradeFormat))
  spreadSub.bind(spreadEvt, myEventListener[SpreadEvt](producer, spreadTop)(spreadFormat))

  /* Poll additional sources for psuedo real-time ccy data */
  val pollThread = new Thread(){
    override def run() = {
      val bitstampPoller  = myEventListener[BTCTicker](producer, bitstampTickerTop)(btcTickerFormat)
      val cmcPoller = myEventListener[TickerEvt](producer, tickerTop)(tickerFormat)

      while(true) {
        bitstampPoller.poll(bitstampTickerUrl)(false)
        cmcPoller.poll(s"${prefix}${tickerEndpoint}")(true)
        Thread.sleep(rateLimit)
      }
    }
  }

  pollThread.run()
  pollThread.join()


  def init = {
    val pusher = new Pusher(apiKey)
    val producer = new KafkaProducer[String, Array[Byte]](Util.loadPropertiesFile(producerProperties))

    (pusher, producer)
  }
}
