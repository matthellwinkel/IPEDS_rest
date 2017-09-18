import com.mongodb.MongoClient
import com.mongodb.client.model.Filters.eq
import org.bson.Document
import java.lang.Integer.parseInt

val mongo = MongoClient("localhost", 27017)

fun FetchSchools(max: Int) : List<School> {
    val list = mutableListOf<School>()
    val s = mongo.getDatabase("ipeds_2016").getCollection("Schools").find().limit(max)
    s.forEach { school ->
        val c = mongo.getDatabase("ipeds_2016").getCollection("Charges").find(eq("UNITID", school.getInteger("UNITID"))).firstOrNull()
        list.add(mapSchool(school, c)!!)
    }
    println(list)
    return list
}

fun FetchSchoolFromId(id: String) : School? {
    val schoolId: Int? = try {
        parseInt(id)
    } catch (e: NumberFormatException) {
        null
    }

    // Grab the school data here
    val s = mongo.getDatabase("ipeds_2016").getCollection("Schools").find(eq("UNITID", schoolId)).first()
    val c = mongo.getDatabase("ipeds_2016").getCollection("Charges").find(eq("UNITID", schoolId)).firstOrNull()
    val school = if (s != null) mapSchool(s, c) else null
    println("School: $school")
    return school
}

fun mapSchool(school: Document, charges: Document?) : School? {
    return School(id = school.getInteger("UNITID"),
            name = school.getString("INSTNM"),
            address = Address(addr1 = school.getString("ADDR"),
                    city = school.getString("CITY"),
                    state = school.getString("STABBR"),
                    zip = school.get("ZIP").toString(), // data has int & string "12345-2323" | 12345
                    lon = school.getDouble("LONGITUD"),
                    lat = school.getDouble("LATITUDE")),
            websites = Websites(main = school.getString("WEBADDR"),
                    adminURL = school.getString("ADMINURL"),
                    faidURL = school.getString("FAIDURL"),
                    appURL = school.getString("APPLURL"),
                    npricURL = school.getString("NPRICURL"),
                    vetURL = school.getString("VETURL"),
                    athURL = school.getString("ATHURL")),
            charges = Charges(pubTuition = charges?.get("CHG1PY3") as? Int, // data has "." in some fileds
                    booksSupplies = charges?.get("CHG4PY3") as? Int, // this 'safe' cast should replace with null
                    onCRoomBoard = charges?.get("CHG5PY3") as? Int,
                    onCOther = charges?.get("CH6PY3") as? Int,
                    offCRoomBoard = charges?.get("CHG7PY3") as? Int,
                    offCOther = charges?.get("CHG8PY3") as? Int,
                    offCFOther = charges?.get("CHG9PY3") as? Int
            )
    )
}

data class School(val id: Int,
                  val name: String,
                  val address: Address,
                  val websites: Websites,
                  val charges: Charges?
                )

data class Address(val addr1: String,
                    val city: String,
                    val state: String,
                    val zip: String,
                    val lon: Double,
                    val lat: Double)

data class Websites(val main: String?,    //Institution's internet website address
                val adminURL: String?,   //Admissions office web address
                val faidURL: String?,    //Financial aid office web addresss
                val appURL: String?,     //Online application web addres
                val npricURL: String?,   //Net price calculator web address
                val vetURL: String?,     //Veterans and Military Servicemembers tuition policies web address
                val athURL: String?      //Student-Right-to-Know student athlete graduation rate web address,
                )

data class Charges(val pubTuition: Int?,
                   val booksSupplies: Int?,
                   val onCRoomBoard: Int?,
                   val onCOther: Int?,
                   val offCRoomBoard: Int?,
                   val offCOther: Int?,
                   val offCFOther: Int?)
