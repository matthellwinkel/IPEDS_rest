import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.*
import org.bson.Document
import java.lang.Integer.parseInt

val mongo = MongoClient("localhost", 27017)

fun FetchSchools(max: Int, year: String = "2016", state: String? = null, pattern: String? = null): List<School> {
    println("Max: $max , Year: $year , State: $state , Pattern: $pattern")
    val list = mutableListOf<School>()
    val s = mongo.getDatabase("ipeds_$year").getCollection("Schools").find().limit(max)
    if (!state.isNullOrEmpty()) {
        s.filter(regex("STABBR", "MT"))
    }
    if (!pattern.isNullOrEmpty()) {
        s.filter(or(regex("INSTNM", ".*$pattern.*"),
                regex("IALIAS", ".*$pattern.*")))
    }
    s.forEach { school ->
        list.add(mapSchool(school, year)!!)
    }
    println(list)
    return list
}

fun FetchSchoolFromId(id: String, year: String = "2016"): School? {
    val schoolId: Int? = try {
        parseInt(id)
    } catch (e: NumberFormatException) {
        null
    }

    // Grab the school data here
    val s = mongo.getDatabase("ipeds_$year").getCollection("Schools").find(eq("UNITID", schoolId)).first()
    val school = if (s != null) mapSchool(s, year) else null
    println("School: $school")
    return school
}

fun mapSchool(school: Document, year: String): School? {
    /*****************************************************************
     *  The IPEDS data has some weirdness and a ton of fields that we really don't care about.
     *  Rather than spending time to massage this data when loading it to mongo, I simply just import
     *  the ipeds csv files directly as documents.
     *  This prevents using some sort of object binding, but we can just manually assign these values to our
     *  objects here, doing some consistency checks.
     *******************************************************************/

    return School(id = school.getInteger("UNITID"),
            name = school.getString("INSTNM"),
            address = Address(addr1 = school.getString("ADDR"),
                    city = school.getString("CITY"),
                    state = school.getString("STABBR"),
                    zip = school.get("ZIP").toString(), // data has int & string "12345-2323" | 12345
                    lon = school.getDouble("LONGITUD"),
                    lat = school.getDouble("LATITUDE")),
            websites = Websites(main = school.getString("WEBADDR"),
                    adminUrl = if (!school.getString("ADMINURL").isNullOrBlank())
                        school.getString("ADMINURL") else null,
                    faidUrl = if (!school.getString("FAIDURL").isNullOrBlank())
                        school.getString("FAIDURL") else null,
                    appUrl = if (!school.getString("APPLURL").isNullOrBlank())
                        school.getString("APPLURL") else null,
                    npricUrl = if (!school.getString("NPRICURL").isNullOrBlank())
                        school.getString("NPRICURL") else null,
                    vetUrl = if (!school.getString("VETURL").isNullOrBlank())
                        school.getString("VETURL") else null,
                    athUrl = if (!school.getString("ATHURL").isNullOrBlank())
                        school.getString("ATHURL") else null),
            charges = mapCharges(school.getInteger("UNITID"), year),
            completions = getCompletions(school.getInteger("UNITID"), year),
            dataYear = year
    )
}


fun mapCharges(id: Int, year: String): Charges? {
    val c = mongo.getDatabase("ipeds_$year").getCollection("Charges").find(eq("UNITID", id)).firstOrNull()
    val vocationalColumn = "CHG1PY3"
    val academicColumn = "CHG1AY3"

    val charges = Charges(
            // this is a weird conditional to see, but Boolean? = null can't evaluate to Boolean
            vocational = if (c?.keys?.contains(vocationalColumn) == true)
                            Vocational(pubTuition = c["CHG1PY3"] as? Int, // data has "." in some fileds
                                booksSupplies = c["CHG4PY3"] as? Int, // this 'safe' cast should replace with null
                                onCRoomBoard = c["CHG5PY3"] as? Int,
                                onCampusOther = c["CH6PY3"] as? Int,
                                offCampusRoomBoard = c["CHG7PY3"] as? Int,
                                offCampusOther = c["CHG8PY3"] as? Int,
                                offCampusWFamilyOther = c["CHG9PY3"] as? Int)
                        else null,
            academic = if (c?.keys?.contains(academicColumn) == true)
                            Academic(pubInDistrictTuition = c["CHG1AT3"] as? Int,
                                    pubInDistrictFees = c["CHG1AF3"] as? Int,
                                    pubInDistrictTuitionAndFees = c["CHG1AY3"] as? Int,  // this is just the sum???
                                    pubInStateTution = c["CHG2AT3"] as? Int,
                                    pubInStateFees = c["CHG2AF3"] as? Int,
                                    pubInStateTuitionAndFees = c["CHG2AY3"] as? Int,
                                    pubOutStateTution = c["CHG3AT3"] as? Int,
                                    pubOutStateFees = c["CHG3AF3"] as? Int,
                                    pubOutStateTuitionAndFees = c["CHG3AY3"] as? Int,
                                    booksSupplies = c["CHG4AY3"] as? Int,
                                    onCampusRoomBoard = c["CHG5AY3"] as? Int,
                                    onCampusOther = c["CHG6AY3"] as? Int,
                                    offCampusRoomBoard = c["CHG7AY3"] as? Int,
                                    offCampusOther = c["CHG8AY3"] as? Int,
                                    offCampusWFamilyOther = c["CHG9AY3"] as? Int,
                                    inDistrictAvgTuitionUgrad = c["TUITION1"] as? Int,
                                    inStateAvgTuitionUgrad = c["TUITION2"] as? Int,
                                    outStateAvgTutionUgrad = c["TUITION3"] as? Int,
                                    inDistrictAvgTuitionGrad = c["TUITION5"] as? Int,
                                    inStateAvgTuitionGrad = c["TUITION6"] as? Int,
                                    outStateAvgTutionGrad = c["TUITION7"] as? Int)
                        else null
    )

    return charges
}

fun getCompletions(id: Int, year: String): Completions? {
    val c = mongo.getDatabase("ipeds_$year").getCollection("Completions").find(eq("UNITID", id)).firstOrNull()
    val completions = if (c != null) Completions(grandTot = c["CSTOTLT"] as? Int,
                                totMen = c["CSTOTLM"] as? Int,
                                totWomen = c["CSTOTLW"] as? Int,
                                nativeMen = c["CSAIANM"] as? Int,
                                nativeWomen = c["CSAIANW"] as? Int,
                                asianMen = c["CSASIAM"] as? Int,
                                asianWomen = c["CSASIAW"] as? Int,
                                blackMen = c["CSBKAAM"] as? Int,
                                blackWomen = c["CSBKAAW"] as? Int,
                                hispMen = c["CSHISPM"] as? Int,
                                hispWomen = c["CSHISPW"] as? Int,
                                pacIslanderMen = c["CSNHPIM"] as? Int,
                                pacIslanderWomen = c["CSNHPIW"] as? Int,
                                whiteMen = c["CSWHITM"] as? Int,
                                whiteWomen = c["CSWHITW"] as? Int)
                    else null
    return completions
}


data class School(val id: Int,
                  val name: String,
                  val address: Address,
                  val websites: Websites,
                  val charges: Charges?,
                  val completions: Completions?,
                  val dataYear: String
)

data class Address(val addr1: String,
                   val city: String,
                   val state: String,
                   val zip: String,
                   val lon: Double,
                   val lat: Double)

data class Websites(val main: String?, //Institution's internet website address
                    val adminUrl: String?, //Admissions office web address
                    val faidUrl: String?, //Financial aid office web addresss
                    val appUrl: String?, //Online application web addres
                    val npricUrl: String?, //Net price calculator web address
                    val vetUrl: String?, //Veterans and Military Servicemembers tuition policies web address
                    val athUrl: String?      //Student-Right-to-Know student athlete graduation rate web address,
)

data class Charges(
        // The data for Vocational/Academic charges is in the same collection (IC2016_AY, IC2016_PY ipeds tables)
        val vocational: Vocational?,
        val academic: Academic?
        )

    data class Vocational(val pubTuition: Int?,
                          val booksSupplies: Int?,
                          val onCRoomBoard: Int?,
                          val onCampusOther: Int?,
                          val offCampusRoomBoard: Int?,
                          val offCampusOther: Int?,
                          val offCampusWFamilyOther: Int?
                        )

    data class Academic(val pubInDistrictTuition: Int?,
                        val pubInDistrictFees: Int?,
                        val pubInDistrictTuitionAndFees: Int?,

                        val pubInStateTution: Int?,
                        val pubInStateFees: Int?,
                        val pubInStateTuitionAndFees: Int?,

                        val pubOutStateTution: Int?,
                        val pubOutStateFees: Int?,
                        val pubOutStateTuitionAndFees: Int?,

                        val booksSupplies: Int?,
                        val onCampusRoomBoard: Int?,
                        val onCampusOther: Int?,
                        val offCampusRoomBoard: Int?,
                        val offCampusOther: Int?,
                        val offCampusWFamilyOther: Int?,

                        val inDistrictAvgTuitionUgrad: Int?,
                        val inStateAvgTuitionUgrad: Int?,
                        val outStateAvgTutionUgrad: Int?,
                        val inDistrictAvgTuitionGrad: Int?,
                        val inStateAvgTuitionGrad: Int?,
                        val outStateAvgTutionGrad: Int?

                        )

data class Completions(val grandTot: Int?, // this can probably be derived also, but its here so...
                       val totMen: Int?,
                       val totWomen: Int?,
                       val nativeMen: Int?,
                       val nativeWomen: Int?,
                       val asianMen: Int?,
                       val asianWomen: Int?,
                       val blackMen: Int?,
                       val blackWomen: Int?,
                       val hispMen: Int?,
                       val hispWomen: Int?,
                       val pacIslanderMen: Int?,
                       val pacIslanderWomen: Int?,
                       val whiteMen: Int?, // can't jump
                       val whiteWomen: Int?)
