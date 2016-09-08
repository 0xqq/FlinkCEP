import Model.{BTCTicker, SpreadEvt, TickerEvt, TradeEvt}
import spray.json.DefaultJsonProtocol._

/**
  * Created by M.Sumner on 20/06/2016.
  */
object Const {

  val apiKey = "de504dc5763aeef9ff52"
  val rateLimit = 12000L
  val (tradeTop, tradeCh, tradeEvt)    = ("btctrades", "live_trades", "trade")
  val (spreadTop, spreadCh, spreadEvt)  = ("spreads", "diff_order_book", "data")
  val (tickerTop, prefix, tickerEndpoint, globalEndpoint) = (
    "ticker",
    "https://api.coinmarketcap.com/v1/",
    "ticker",
    "global")

  val (bitstampTickerTop, bitstampTickerUrl) = ("bitstampTickerTop", "https://www.bitstamp.net/api/v2/ticker/btcusd/")

  val tradeFormat = jsonFormat(TradeEvt.apply, "price", "timestamp", "amount", "type", "id")
  val spreadFormat = jsonFormat(SpreadEvt.apply, "timestamp", "bids", "asks")
  val tickerFormat = jsonFormat(TickerEvt.apply, "id", "name", "symbol", "rank", "price_usd",
    "24h_volume_usd", "market_cap_usd", "available_supply", "total_supply", "percent_change_1h", "percent_change_24h", "percent_change_7d")
  val btcTickerFormat = jsonFormat(BTCTicker.apply, "high", "last", "timestamp", "bid", "vwap", "volume", "low", "ask", "open")

  val producerProperties = "src/main/resources/producer.properties"

}
