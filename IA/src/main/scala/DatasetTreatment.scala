import com.github.javafaker.Faker
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDate
import scala.util.Random

case class DatasetTreatment(dsSource: String) {
  /* Loading the datafile : */
  val model = ModelFactory.createDefaultModel()
  val faker = new Faker();


  val vaccines = List("Pfizer", "Moderna", "AstraZeneca", "SpoutnikV", "CanSinoBio")

  val typeProp = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val studRes = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent"

  val identifierProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Identifier"
  val firstNameProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#FirstName"
  val lastNameProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#LastName"
  val genderProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Gender"
  val zipcodeProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Zipcode"
  val birthdayProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Birthday"
  val vdProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#VaccineDate"
  val vnProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#VaccineName"

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

  def numberOfStudents() = {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(studRes)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.toList.size()
  }

  def fullStudents() = {
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
      val iter7 = model.listObjectsOfProperty(x.asResource(), model.createProperty(vdProp)).toList
      val iter8 = model.listObjectsOfProperty(x.asResource(), model.createProperty(vnProp)).toList
      println(iter.get(0) + " " + iter2.get(0) + " " + iter3.get(0) + " " + iter4.get(0) + " " + iter5.get(0) + " " + iter6.get(0) + " " + iter7.get(0) + " " + iter8.get(0))
    })
  }

  def addIdentifier(x: Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(identifierProp),
      model.createResource(faker.idNumber().valid())
    ))
  }

  def addFirstName(x: Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(firstNameProp),
      model.createResource(faker.name().firstName()))
    )
  }

  def addLastName(x: Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(lastNameProp),
      model.createResource(faker.name().lastName()))
    )
  }

  def addGender(x: Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(genderProp),
      model.createResource(faker.demographic().sex()))
    )
  }

  def addZipcode(x: Resource) = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(zipcodeProp),
      model.createResource(faker.address().zipCode()))
    )
  }

  def addBirthday(x: Resource, startAge: Int, finishAge: Int): Model = {
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(birthdayProp),
      model.createResource(faker.date().birthday(startAge, finishAge).toString))
    )
  }

  def addVaccineDate(x: Resource, pr: Boolean): Model = {
    if (pr) {
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vdProp),
        model.createResource(faker.date().between(new SimpleDateFormat("dd-MM-yyyy").parse("01-01-2020"),
          new SimpleDateFormat("dd-MM-yyyy").parse("01-01-2022")).toString))
      )
    }
    else {
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vdProp),
        model.createResource("False"))
      )
    }
  }

  def addVaccineName(x: Resource, pr: Boolean): Model = {

    if (pr) {
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vnProp),
        model.createResource(vaccines(faker.random().nextInt(vaccines.size)))
      ))
    }
    else {
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vnProp),
        model.createResource("None")
      ))
    }

  }


  // Need to add the others datas and to create it for teachers
  def addDataToStudents(vaccinedProportion: Int): Unit = {
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
      val pr = Random.nextInt(100) < vaccinedProportion
      addVaccineDate(x, pr)
      addVaccineName(x, pr)

    })
  }
}