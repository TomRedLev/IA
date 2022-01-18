import com.github.javafaker.Faker
import org.apache.jena.rdf.model.{ModelFactory, Resource}

case class DatasetTreatment(dsSource : String) {
  /* Loading the datafile : */
  val model = ModelFactory.createDefaultModel()
  val faker = new Faker();

  val typeProp = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val studRes = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent"

  val studFirstNameProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#StudentFirstName"
  val studLastNameProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#StudentLastName"
  val studIdentifierProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#StudentName"

  def load() = model.read(dsSource, "TTL")

  def size() = model.size()

  /* Testing Java Faker : */
  def testFaker() = {
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
  }

  def firstNameOfStudents()= {
    val rdfType = model.createProperty(studFirstNameProp)
    val iterator = model.listObjectsOfProperty(rdfType).toList
    iterator.forEach(println)
  }

  def addIdentifier(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(studIdentifierProp),
      model.createResource(faker.idNumber().valid())
    ))
  }

  def addFirstName(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(studFirstNameProp),
      model.createResource(faker.name().firstName()))
    )
  }

  def addLastName(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(studLastNameProp),
      model.createResource(faker.name().lastName()))
    )
  }

  def addGender(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(studGenderProp),
      model.createResource(faker
    )
  }


  // Need to add the others datas and to create it for teachers
  def addDataToStudents(): Unit = {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(studRes)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.forEach(x => {
      addIdentifier(x)
      addFirstName(x)
      addLastName(x)
    })
  }
}