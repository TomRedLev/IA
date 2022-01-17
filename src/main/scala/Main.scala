import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.impl.PropertyImpl

import scala.collection.mutable.ListBuffer

object Main extends App {
    val model = ModelFactory.createDefaultModel()
    model.read("file:lubm1.ttl", "TTL")
    println(model.size())
}