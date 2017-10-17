##' Databricks Python Engine for Knitr
##'@export
databricksPythonEngine <- function(options){
    ## if(!(!is.null(options$dbx) && options$dbx==TRUE)){
    ##     return(getOption("databricksOldPythonEgine")(options))
    ## }
    if(is.null(getOption("dbpycontext"))){
        r <- dbxCtxMake()
        while(TRUE){
            ctxStats <- dbxCtxStatus(r)
            if(isContextRunning(ctxStats)) break
        }
        options(dbpycontext=r)
    }
    ctx <- getOption("dbpycontext")
    if (paste(options$code, sep = "", collapse = "") == "")
        return(knitr::engine_output(options, options$code, NULL, NULL))
    code <- paste(options$code, sep = "", collapse = "\n")
    extra = NULL
    out <- NULL
    print(options$code)
    print(options$eval)
    if(options$eval){
        cid3 <- dbxRunCommand(code,ctx=ctx,language='python',wait=3)
        if(cid3$results$resultType=="error"){
            (out <- cid3$results$cause)
        }else if(cid3$results$resultType=="image"){
            system(sprintf("dbfs cp dbfs:/FileStore%s %s/", cid3$results$fileName,tempdir()))
            f <- sprintf("%s/%s",tempdir(),basename(cid3$results$fileName))
            extra <- sapply(f, function(f) knitr::knit_hooks$get("plot")(f, options))
            out <- cid3$results$data
            if(is.null(out)) out <- ""
        }else if(cid3$results$resultType=="text"){
            out <- cid3$results$data
        }
    }
    options$engine='python'
    knitr::engine_output(options, options$code, out,extra)
}

##' Databricks R Engine Knitr
##'@export
databricksREngine <- function(options){
    ## if(!(!is.null(options$dbx) && options$dbx==TRUE)){
    ##     return(getOption("databricksOldPythonEgine")(options))
    ## }
    if(is.null(getOption("dbRcontext"))){
        r <- dbxCtxMake(language='r')
        while(TRUE){
            ctxStats <- dbxCtxStatus(r)
            if(isContextRunning(ctxStats)) break
        }
        options(dbRcontext=r)
    }
    ctx <- getOption("dbRcontext")
    if (paste(options$code, sep = "", collapse = "") == "")
        return(knitr::engine_output(options, options$code, NULL, NULL))
    code <- paste(options$code, sep = "", collapse = "\n")
    extra = NULL
    out <- NULL
    if(options$eval){
    cid3 <- dbxRunCommand(code,ctx=ctx,language='r',wait=3)
    if(cid3$results$resultType=="error"){
        (out <- cid3$results$cause)
    }else if(cid3$results$resultType=="image"){
        system(sprintf("dbfs cp dbfs:/FileStore%s %s/", cid3$results$fileName,tempdir()))
        f <- sprintf("%s/%s",tempdir(),basename(cid3$results$fileName))
        extra <- sapply(f, function(f) knitr::knit_hooks$get("plot")(f, options))
    out <- cid3$results$data
        if(is.null(out)) out <- ""
    }else if(cid3$results$resultType=="text"){
        out <- cid3$results$data
    }
    }
    options$engine='R'
    knitr::engine_output(options, options$code, out,extra)
}
