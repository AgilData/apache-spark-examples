
object CensusSchema {

  def parseGeo(s: String) = {
    Geo(
      s.substring(18,25), // Logical Record No
      s.substring(226,316).trim, // Name
      s.substring(318,327), // population
      s.substring(8,11) // Summary Level (050 is county)
    )
  }

}

case class Geo(logrecno: String, name: String, population: String, sumlev: String)

case class CountyPopulation(county: String, male: Int, female: Int)