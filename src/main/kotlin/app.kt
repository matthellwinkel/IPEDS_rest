import com.google.gson.GsonBuilder
import spark.Filter
import spark.Spark.*



fun main(args: Array<String>) {
    port(9091)

    staticFiles.location("/static")

    // allow routes to end in slash
    before(Filter({req, res ->
        val path = req.pathInfo()
        if (!path.equals("/") && path.endsWith("/")){
            res.redirect(path.substring(0, path.length -1))
        }
    }))

    //set response type to json for api routes
    after(Filter({req, res ->
        if(req.pathInfo().startsWith("/api")){
            res.type("application/json")
        }
    }))

    //gzip everything
    after(Filter({req, res ->
        res.header("Content-Encoding", "gzip")
    }))

    val gson = GsonBuilder().serializeNulls().setPrettyPrinting().create()

    /**
     *  Fetch a list of Schools. We default to a max number here.
     *  TODO: this route should also provide use of query params to search for specific schools
     */
    get("/api/v1/schools", { req, _ ->
        // set max or default
        val max = if ("limit" in req.queryParams()) req.queryParams("limit").toInt() else 10

        // read other query params to try to set search fields
        req.queryParams().filter{ parm -> parm != "limit"}.forEach { parm ->
            //kotlin switch is pretty useful here
            when(parm) {
               "test" -> println(" setting search filter: $parm")
            }
        }
        FetchSchools(max)
    }, { gson.toJson(it)})


    /**
     *  Fetch a specific school by ID
     */
    get("/api/v1/schools/:id", { req, _ ->
        FetchSchoolFromId(req.params(":id"))
    }, { gson.toJson(it)})
}