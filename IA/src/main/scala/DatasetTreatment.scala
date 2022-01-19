import com.github.javafaker.Faker
import org.apache.jena.rdf.model.{ModelFactory, Resource}

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

case class DatasetTreatment(dsSource : String) {
  /* Loading the datafile : */
  val model = ModelFactory.createDefaultModel()
  val faker = new Faker();

  val typeProp = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val studRes = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent"

  val identifierProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Identifier"
  val firstNameProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#FirstName"
  val lastNameProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#LastName"
  val genderProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Gender"
  val zipcodeProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Zipcode"
  val birthdayProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Birthday"

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

  def numberOfStudents()= {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(studRes)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.toList.size()
  }

  def fullStudents()= {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(studRes)
    val iterator = model.listSubjectsWithProperty(rdfType, obj).toList
    iterator.forEach(x => {
      val iter = model.listObjectsOfProperty(x.asResource(), model.createProperty(identifierProp)).toList
      val iter2 = model.listObjectsOfProperty(x.asResource(), model.createProperty(firstNameProp)).toList
      val iter3 = model.listObjectsOfProperty(x.asResource(), model.createProperty(lastNameProp)).toList
      val iter4 = model.listObjectsOfProperty(x.asResource(), model.createProperty(genderProp)).toList
      val iter5 = model.listObjectsOfProperty(x.asResource(), model.createProperty(zipcodeProp)).toList
      val iter6 = model.listObjectsOfProperty(x.asResource(), model.createProperty(birthdayProp)).toList
      println(iter.get(0) + " " + iter2.get(0) + " " + iter3.get(0) + " " + iter4.get(0) + " " + iter5.get(0) + " " + iter6.get(0))
    })
  }

  def addIdentifier(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(identifierProp),
      model.createResource(faker.idNumber().valid())
    ))
  }

  def addFirstName(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(firstNameProp),
      model.createResource(faker.name().firstName()))
    )
  }

  def addLastName(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(lastNameProp),
      model.createResource(faker.name().lastName()))
    )
  }

  def addGender(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(genderProp),
      model.createResource(faker.demographic().sex()))
    )
  }

  def addZipcode(x : Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(zipcodeProp),
      model.createResource(faker.address().zipCode()))
    )
  }

  def addBirthday(x : Resource, startAge : Int, finishAge : Int) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(birthdayProp),
      model.createResource(faker.date().birthday(startAge, finishAge).toString)
    ))
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
      addGender(x)
      addZipcode(x)
      addBirthday(x, 20, 30)
      //addVaccineDate(x)
      //addVaccineName(x)
    })
  }
}