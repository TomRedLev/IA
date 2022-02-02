object Main extends App {
  val ds = DatasetTreatment("file:lubm1.ttl")
  ds.load()
  println(ds.size()) //Commented
  ds.testFaker() //Commented
  println(ds.numberOfStudents()) //Commented
  //ds.addDataToStudents(100, 100, 100)
  //ds.fullStudents() //Commented
  ds.addDatasToEveryone(100, 100, 100)
  ds.save("final_version.ttl")
}
