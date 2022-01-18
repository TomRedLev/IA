import com.github.javafaker.Faker
import org.apache.jena.rdf.model.ModelFactory

class DatasetTreatment(val dsSource : String) {
  /* Loading the datafile : */
  val model = ModelFactory.createDefaultModel()
  val typeProp = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val studRes = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent"


  def load() = model.read(dsSource, "TTL")

  def size() = model.size()

  /* Testing Java Faker : */
  def testFaker() = {
    val faker = new Faker();
    val name = faker.name().fullName();
    val firstName = faker.name().firstName();
    val lastName = faker.name().lastName();
    val streetAddress = faker.address().streetAddress();
    println(name + ", " + firstName + ", " + lastName + ", " + streetAddress)
  }

  /* Checking the number of students : */
  def numberOfStudents()= {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(studRes)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.toList.size()
    //iterator.forEach(x => println(x))
  }
}

object DatasetTreatment {
  def apply(dsSource : String) = new DatasetTreatment(dsSource)
}