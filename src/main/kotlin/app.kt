import com.google.gson.FieldNamingPolicy
import com.google.gson.GsonBuilder
import spark.Filter
import spark.Spark.*



fun main(args: Array<String>) {
    port(9091)

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
    after(Filter({_, res ->
        res.header("Content-Encoding", "gzip")
    }))

    val gson = GsonBuilder()
                .serializeNulls()
                .setPrettyPrinting()
                .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                .create()

    /**
     *  Fetch a list of Schools. We default to a max number here.
     *  TODO: this route should also provide use of query params to search for specific schools
     */
    get("/api/v1/schools", { req, _ ->
        // set max or default

        FetchSchools(max = req.queryParams("limit")?.toInt() ?: 10,
                    state = req.queryParams("state"),
                    pattern = req.queryParams("name"))
    }, { gson.toJson(it)})


    /**
     *  Fetch a specific school by ID
     */
    get("/api/v1/schools/:id", { req, _ ->
        val year = req.queryParams("year")
        println("year: $year")
        FetchSchoolFromId(req.params(":id"), year)
    }, { gson.toJson(it)})
}