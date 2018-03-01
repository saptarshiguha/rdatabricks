ul <- function(s,f,h=as.character) h(unlist(lapply(s,f)))
tryParsing <- function(r){
    s <- content(r)
    if(is.character(s)) {
        rq = fromJSON(s)
        err = sprintf("%s:: %s",rq$error_code, rq$message)
        return(list(status=FALSE,content=err))
     } else{
        return(list(status=TRUE, content=fromJSON(s)))
     }
 }
##' Get the different versions of spark available
##' @param token obtained from databricks
##' @param instance from frank
##' @details see https://docs.databricks.com/api/latest/clusters.html#spark-versions
##' @export
dbxSparkVersions<-
    function(token = options("databricks")[[1]]$token
            ,instance = options("databricks")[[1]]$instance)
{

    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/spark-versions",instance=instance)
    res <- GET(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)))
    res <- fromJSON(content(res,'text'))
    if(!is.null(res$error_code)){
        stop(infuse("dbx: {{code}}:{{msg}}",code=res$error_code,msg=res$message))
    }else {
        defaultVersion <- uu$default_version_key
        f <- data.table(keys = unlist(lapply(uu$versions,"[[","key")),
                        name = unlist(lapply(uu$versions,"[[","name")))
        f[order(keys),]
        f[, default:=FALSE]
        list(default = defaultVersion, versions=f)
    }
}

##' Get the different zones
##' @param token obtained from databricks
##' @param instance from frank
##' @details see https://docs.databricks.com/api/latest/clusters.html#list-zones
##' @return a list of default zone and data table of zones
##' @export
dbxZones<-
    function(token = options("databricks")[[1]]$token
            ,instance = options("databricks")[[1]]$instance)
{

    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/list-zones",instance=instance)
    res <- GET(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)))
    res <- fromJSON(content(res,'text'))
    if(!is.null(res$error_code)){
        stop(infuse("dbx: {{code}}:{{msg}}",code=res$error_code,msg=res$message))
    }else {
        f <- data.table(zones= res$zones)
        list(default = res$default_zone, zones=f)
    }
}

##' Get the different node types
##' @param token obtained from databricks
##' @param instance from frank
##' @details seehttps://docs.databricks.com/api/latest/clusters.html#list-node-types
##' @return a list with default node type and data table of nodes
##' @export
dbxNodeTypes<-
    function(token = options("databricks")[[1]]$token
            ,instance = options("databricks")[[1]]$instance)
{

    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/list-node-types",instance=instance)
    res <- GET(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)))
    res <- fromJSON(content(res,'text'))
    if(!is.null(res$error_code)){
        stop(infuse("dbx: {{code}}:{{msg}}",code=res$error_code,msg=res$message))
    }else {
        ## see https://docs.databricks.com/api/latest/clusters.html#clusternodetype
        x <- res$node_types
        f <- data.table(
            node_type_id = ul(x,function(k) k$node_type_id),
            memory_mb = ul(x,function(k) k$memory_mb,as.numeric),
            num_cores = ul(x,function(k) k$num_cores,as.numeric),
            description = ul(x,function(k) k$description),
            instance_type_id = ul(x,function(k) k$instance_type_id),
            is_deprecated = ul(x,function(k) k$is_deprecated,as.logical)
        )
        setkey(f,"node_type_id")
        return(list(default = res$default_node_type_id, nodes =f))
    }
}

##' Delete a cluster
##' @param cluster_id a string
##' @details see https://docs.databricks.com/api/latest/clusters.html#delete
##' @return nothing
##' @export
dbxDelete <- function(cluster_id
                    , token = options("databricks")[[1]]$token
                    , instance = options("databricks")[[1]]$instance)
{


    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/delete",instance=instance)
    body <- list("cluster_id" = as.character(cluster_id))
    
    res <- POST(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)),
                body = body
              , encode = "json")
content(res,as='parsed')
}


##' Restart a cluster
##' @param cluster_id a string
##' @details see https://docs.databricks.com/api/latest/clusters.html#restart
##' @return nothing
##' @export
dbxRestart <- function(cluster_id
                    , token = options("databricks")[[1]]$token
                    , instance = options("databricks")[[1]]$instance)
{


    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/restart",instance=instance)
    body <- list("cluster_id" = as.character(cluster_id))
    res <- POST(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)),
                body = body
              , encode = "json")
content(res,as='parsed')
}


##' Starts a terminated Spark cluster given its id
##' @param cluster_id a string
##' @details see https://docs.databricks.com/api/latest/clusters.html#restart
##' @return nothing
##' @export
dbxStart <- function(cluster_id
                    , token = options("databricks")[[1]]$token
                    , instance = options("databricks")[[1]]$instance)
{


    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/start",instance=instance)
    body <- list("cluster_id" = as.character(cluster_id))
    res <- POST(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)),
                body = body
              , encode = "json")
content(res,as='parsed')
}

##' Returns an autoscale structure
##' @param  min_workers The minimum number of workers to which the cluster can scale down when underutilized. It is also the initial number of workers the cluster will have after creation.
##' @param max_workers The maximum number of workers 
##' @details see https://docs.databricks.com/api/latest/clusters.html#clusterautoscale
##' @return an object of class autoscale
##' @export
autoscale <- function(min_workers, max_workers)
{
    x <- list(min_workers=as.integer(min_workers),
              max_workers=as.integer(max_workers))
    if(max_workers < min_workers) stop("max must be larger than min")
    class(x) = "autoscale"
    return(x)
}



##' Resize a Spark cluster given its id and num_workers
##' @param cluster_id a string
##' @param num_workers an integer or of class autoscale
##' @details see https://docs.databricks.com/api/latest/clusters.html#resize
##' @return nothing
##' @export
dbxResize <- function(cluster_id
                    , token = options("databricks")[[1]]$token
                    , instance = options("databricks")[[1]]$instance)
{


    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/resize",instance=instance)
    if(!(is(num_workers,"integer") || is(num_workers,"autoscale"))){
        stop("num_workers must be an integer or of class autoscale")
    }
    body <- list("cluster_id" = as.character(cluster_id))
    if(is(num_workers,"integer")){
        body$num_workers = num_workers
    }else{
        body$autoscale <- num_workers
    }
    res <- POST(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)),
                body = body,
              , encode = "json")
content(res,as='parsed')
}
                                  
                                  
                                  ##' Get information about  a cluster
##' @param cluster_id a string
##' @details see https://docs.databricks.com/api/latest/clusters.html#get
##' @return a big json
##' @export
dbxGet <- function(cluster_id
                    , token = options("databricks")[[1]]$token
                    , instance = options("databricks")[[1]]$instance)
{


    if(is.null(token) || is.null(instance)) stop("Must provide a token and instance")
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/2.0/clusters/get",instance=instance)
    body <- list("cluster_id" = as.character(cluster_id))
    res <- POST(url, add_headers(Authorization= infuse("Bearer {{token}}",token=token)),
                body = body
              , encode = "json")
    content(res,as='parsed')
#   r<- tryParsing(res)
#   if(r$status) r$content else stop(r$content)
}



