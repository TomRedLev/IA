import com.github.javafaker.Faker
import org.apache.jena.ontology.OntModelSpec
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.{FileOutputStream, FileWriter, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

import scala.collection.mutable.ListBuffer

case class DatasetTreatment(dsSource: String) {
  /* Loading the datafile : */
  val model: Model = ModelFactory.createDefaultModel()
  val faker = new Faker()
  val mapper = new ObjectMapper()
  var user = mapper.createObjectNode()
  var users = ListBuffer[ObjectNode]()

  val vaccines = Map("Pfizer" -> 40, "Moderna" -> 30, "AstraZeneca" -> 20, "SpoutnikV" -> 5, "CanSinoBio" -> 5)
  val sideeffects = Map("Injection site pain" -> "C0151828",
    "Fatigue" -> "C0015672",
    "Headhache" -> "C0018681",
    "Muscle pain" -> "C0231528",
    "Chills" -> "C0085593",
    "Joint pain" -> "C0003862",
    "Fever" -> "C0015967",
    "Injection site swelling" -> "C0151605",
    "Injection site redness" -> "C0852625",
    "Nausea" -> "C0027497",
    "Malaise" -> "C0231218",
    "Lymphadenopathy" -> "C0497156",
    "Injection site tenderness" -> "C0863083"
  )

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
  val seProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Sideeffect"
  val siderProp = "http://swat.cse.lehigh.edu/onto/univ-bench.owl#Sider"

  def load(): Model = model.read(dsSource, "TTL")

  def size(): Long = model.size()

  def save(fileName : String): Unit = {
    val out = new FileWriter(fileName)
    try {
      RDFDataMgr.write(new FileOutputStream(fileName), model, RDFFormat.TTL)
    }
    finally {
      try {
        out.close()
      }
      catch {
        case _: IOException =>
      }
    }
  }


  /* Testing Java Faker : */
  def testFaker(): Unit = {
    val name = faker.name().fullName()
    val firstName = faker.name().firstName()
    val lastName = faker.name().lastName()
    val streetAddress = faker.address().streetAddress()
    println(name + ", " + firstName + ", " + lastName + ", " + streetAddress)
  }

  def numberOfStudents(): Int = {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(studRes)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.toList.size()
  }

  def displayEveryone(res : String): Unit = {
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
      val iter9 = model.listObjectsOfProperty(x.asResource(), model.createProperty(seProp)).toList
      val iter10 = model.listObjectsOfProperty(x.asResource(), model.createProperty(siderProp)).toList
      println(res + " " + iter.get(0) + " " + iter2.get(0) + " " + iter3.get(0) + " " + iter4.get(0) + " " + iter5.get(0) +
        " " + iter6.get(0) + " " + iter7.get(0) + " " + iter8.get(0) + " " + iter9.get(0) + " " + iter10.get(0))
    })
  }

  def addIdentifier(x: Resource): Model = {
    val id = faker.idNumber().valid()
    user.put("ID", id)
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(identifierProp),
      model.createResource(id)
    ))
  }

  def addFirstName(x: Resource): Model = {
    val fn = faker.name().firstName()
    user.put("FirstName", fn)
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(firstNameProp),
      model.createResource(fn))
    )
  }

  def addLastName(x: Resource): Model = {
    val ln = faker.name().lastName()
    user.put("LastName", ln)
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(lastNameProp),
      model.createResource(ln))
    )
  }

  def addGender(x: Resource): String = {
    val gender = faker.demographic().sex()
    user.put("Gender", gender)
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(genderProp),
      model.createResource(gender))
    )
    gender
  }

  def addZipcode(x: Resource): Model = {
    val zip = faker.address().zipCode()
    user.put("Zipcode", zip)
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(zipcodeProp),
      model.createResource(zip))
    )
  }

  def addBirthday(x: Resource, startAge: Int, finishAge: Int): Model = {
    val birth = faker.date().birthday(startAge, finishAge).toString
    user.put("Birthdate", birth)
    model.add(model.createStatement(
      model.getResource(x.getURI),
      model.getProperty(birthdayProp),
      model.createResource(birth))
    )
  }

  def addVaccineDate(x: Resource, pr: Boolean): Model = {
    var date = faker.date().between(new SimpleDateFormat("dd-MM-yyyy").parse("01-01-2020"), new Date()).toString
    if (pr) {
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vdProp),
        model.createResource(date))
      )
    }
    else {
      date = "False"
      user.put("VaccineDate", date)
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vdProp),
        model.createResource())
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
      user.put("Vaccine", vaccine)
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vnProp),
        model.createResource(vaccine)
      ))
    }
    else {
      val vaccine = "None"
      user.put("Vaccine", vaccine)
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(vnProp),
        model.createResource(vaccine)
      ))
    }

  }

  def addSideeffect(x: Resource, pr: Boolean): Model = {
    if (pr) {
      var se = ""
      var sider = ""
      while (se.equals("")) {
        breakable {
          for ((k,v) <- sideeffects)  {
            if (Random.nextInt(sideeffects.size) % sideeffects.size == 0) {
              se = k
              sider = v
              break
            }
          }
        }
      }
      user.put("Sideeffect", se)
      user.put("Sider", sider)
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(seProp),
        model.createResource(se)
      ))
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(siderProp),
        model.createResource(sider)
      ))
    }
    else {
      val se = "None"
      val sider = "None"
      user.put("Sideeffect", se)
      user.put("Sider", sider)
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(seProp),
        model.createResource(se)
      ))
      model.add(model.createStatement(
        model.getResource(x.getURI),
        model.getProperty(siderProp),
        model.createResource(sider)
      ))
    }
  }


  // Need to add the others datas and to create it for teachers
  def addDatasToEveryoneBis(vaccinedProportion: Int, vaccinedFemaleProportion : Int, vaccinedMaleProportion : Int, res : String): Unit = {
    val rdfType = model.createProperty(typeProp)
    val obj = model.createResource(res)
    val iterator = model.listSubjectsWithProperty(rdfType, obj)
    iterator.forEach(x => {
      user = mapper.createObjectNode()
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
      addSideeffect(x, pr)
      users += user
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

  def producer() : Unit = {
    val usersList = users.toList
    val props : Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topic = "topic0"
    try {
      for (i <- 0 to usersList.length - 1) {
        producer.send(new ProducerRecord[String, String](topic, i.toString, usersList(i).toString))
        //println(usersList(i).toString)
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
  }

  def consumer() : Unit = {
    val props:Properties = new Properties()
    props.put("group.id", "test")
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer(props)
    val topics = List("topic0")
    try {
      consumer.subscribe(topics.asJava)
      while (true) {
        val records = consumer.poll(10)
        for (record <- records.asScala) {
          println(record.toString)
        }
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      consumer.close()
    }
  }
}