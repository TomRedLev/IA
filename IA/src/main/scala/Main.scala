object Main extends App {
  val ds = DatasetTreatment("file:lubm1.ttl")
  ds.load()
  println(ds.size()) //Commented
  ds.testFaker() //Commented
  println(ds.numberOfStudents()) //Commented
  ds.addDataToStudents(52)
  ds.fullStudents() //Commented
}
