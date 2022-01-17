import com.github.javafaker.Faker
import org.apache.jena.rdf.model.ModelFactory

object Main extends App {
  /* Loading the datafile : */
  val model = ModelFactory.createDefaultModel()
  model.read("file:lubm1.ttl", "TTL")
  println(model.size())

  /* Testing Java Faker : */
  val faker = new Faker();
  val name = faker.name().fullName(); // Miss Samanta Schmidt
  val firstName = faker.name().firstName(); // Emory
  val lastName = faker.name().lastName(); // Barton
  val streetAddress = faker.address().streetAddress();
  println(name + ", " + firstName + ", " + lastName + ", " + streetAddress)
}
