import org.apache.kafka.common.TopicPartition

object Main extends App {
  val ds = DatasetTreatment("file:lubm1.ttl")
  ds.load()
  println(ds.size()) //Commented
  ds.testFaker() //Commented
  println(ds.numberOfStudents()) //Commented
  //ds.addDataToStudents(100, 100, 100)
  //ds.fullStudents() //Commented
  ds.addDatasToEveryone(50, 100, 50)
  ds.save("final_version.ttl")
  ds.producer()
  ds.kafkaStream()
  //ds.consumer(List("AnonymousSideEffect"))
  ds.consumerPartitions(List(new TopicPartition("AnonymousSideEffect5Part", 0)))
  ds.consumerPartitions(List(new TopicPartition("AnonymousSideEffect5Part", 1)))
}

// Commandes à lancer :
// Dans ZooKeeper :
// ./zkServer.sh start ../conf/zoo.cfg
// Dans Kafka :
// ./kafka-server-start.sh -daemon ../config/server.properties
// ./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic topic0
// ./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic AnonymousSideEffect
// ./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic AnonymousSideEffect5Part