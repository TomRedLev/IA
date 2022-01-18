object Main extends App {
  val ds = DatasetTreatment("file:lubm1.ttl")
  ds.load()
  println(ds.size())
  ds.testFaker()
  println(ds.numberOfStudents())
}
