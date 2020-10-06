package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

case class MyState(count: Int = 0, sum: Double = 0){
}

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    // make a connection to Kafka and read (key, value) pairs from it
	
	val sparkConf = new SparkConf().setAppName("KafkaSparkWordCound").setMaster("local[2]")
	val ssc = new StreamingContext(sparkConf, Seconds(10))
	ssc.checkpoint(".checkpoints/")

    val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")

	val topics = Set("avg")

    val messages = KafkaUtils.createDirectStream[
	String, String, StringDecoder, StringDecoder](
	ssc, kafkaConf, topics)
	//messages.print()
	val values = messages.map(x => x._2) //transfor the tuple (null, String) to (String)
	val pairs = values.map(_.split(",")).map(x=> (x(0), x(1).toDouble)) //split the string in 2 values
	pairs.print()

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[MyState]): (String, Double) = {
		if (state.exists() && value.isDefined){
			val newValue = value.get
			val newSum = state.get().sum + newValue
			val newCount = state.get().count + 1
			val newAvg = newSum / newCount
			val newState = MyState(newCount, newSum)
			state.update(newState)
			return (key, newAvg)
		} 
		else if (!state.exists() && value.isDefined){ //first time we process the window (first window)
			val newValue = value.get
			state.update(MyState(1, newValue))
			return (key, newValue)
		} 
		else { //no value is defined
			return ("", 0.0) 
		}
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
	
    ssc.start()
    ssc.awaitTermination()
  }
}
