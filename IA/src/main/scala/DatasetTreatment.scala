import com.github.javafaker.Faker
import org.apache.jena.ontology.OntModelSpec
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}
import org.apache.jena.riot
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}

import java.io.{FileOutputStream, FileWriter, IOException}
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

case class DatasetTreatment(dsSource: String) {
  /* Loading the datafile : */
  val model = ModelFactory.createDefaultModel()
  val faker = new Faker();

  val vaccines = Map("Pfizer" -> 40, "Moderna" -> 30, "AstraZeneca" -> 20, "SpoutnikV" -> 5, "CanSinoBio" -> 5)

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

  def save(fileName : String) = {
    val out = new FileWriter(fileName)
    try {
      RDFDataMgr.write(new FileOutputStream(fileName), model, RDFFormat.TTL);
    }
    finally {
      try {
        out.close()
      }
      catch {
        case e : IOException =>
      }
    }
  }

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

  def displayEveryone(res : String) = {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(res)
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
      println(res + " " + iter.get(0) + " " + iter2.get(0) + " " + iter3.get(0) + " " + iter4.get(0) + " " + iter5.get(0) + " " + iter6.get(0) + " " + iter7.get(0) + " " + iter8.get(0))
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
    val gender = faker.demographic().sex()
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(genderProp),
      model.createResource(gender))
    )
    gender
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
          new Date()).toString))
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
      var vaccine = ""
      while (vaccine.equals("")) {
        breakable {
          for ((k,v) <- vaccines)  {
            if (Random.nextInt(100) < v) {
              vaccine = k
              break
            }
          }
        }
      }
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vnProp),
        model.createResource(vaccine)
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
  def addDatasToEveryoneBis(vaccinedProportion: Int, vaccinedFemaleProportion : Int, vaccinedMaleProportion : Int, res : String): Unit = {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(res)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.forEach(x => {
      addIdentifier(x)
      addFirstName(x)
      addLastName(x)
      val gender = addGender(x)
      addZipcode(x)
      addBirthday(x, 20, 30)
      var pr = false
      if (gender.equals("Female")) {
        pr = Random.nextInt(100) < vaccinedProportion && Random.nextInt(100) < vaccinedFemaleProportion
      } else {
        pr = Random.nextInt(100) < vaccinedProportion &&  Random.nextInt(100) < vaccinedMaleProportion
      }
      addVaccineDate(x, pr)
      addVaccineName(x, pr)
    })
  }


  def addDatasToEveryone(vaccinedProportion: Int, vaccinedFemaleProportion : Int, vaccinedMaleProportion : Int): Unit = {
    val ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF)
    ontology.read("univ-bench.owl")
    val persons = ontology.getOntClass("http://swat.cse.lehigh.edu/onto/univ-bench.owl#Person")
    val sc = persons.listSubClasses(false)
    sc.forEach(x => {
      addDatasToEveryoneBis(vaccinedProportion, vaccinedFemaleProportion, vaccinedMaleProportion, x.getURI)
      displayEveryone(x.getURI)
    })
  }
}