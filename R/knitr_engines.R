##' Engine for Knitr
##'@export
databricksPythonEngine <- function(options){
    if(!(!is.null(options$dbx) && options$dbx==TRUE)){
        return(getOption("databricksOldPythonEgine")(options))
    }
    if(is.null(getOption("dbcontext"))){
        r <- dbxCtxMake()
        while(TRUE){
            ctxStats <- dbxCtxStatus(r)
            if(isContextRunning(ctxStats)) break
        }
        options(dbcontext=r)
    }
    ctx <- getOption("dbcontext")
    if (paste(options$code, sep = "", collapse = "") == "")
        return(knitr::engine_output(options, options$code, NULL, NULL))
    code <- paste(options$code, sep = "", collapse = "\n")
    cid3 <- dbxRunCommand(code,ctx=ctx,wait=3)
    extra = NULL
    if(cid3$results$resultType=="error"){
        out <- cid3$results$cause
    }else if(cid3$results$resultType=="image"){
        system(sprintf("dbfs cp dbfs:/FileStore%s %s/", cid3$results$fileName,tempdir()))
        f <- sprintf("%s/%s",tempdir(),basename(cid3$results$fileName))
        extra <- sapply(f, function(f) knitr::knit_hooks$get("plot")(f, options))
    out <- cid3$results$data
        if(is.null(out)) out <- ""
    }else if(cid3$results$resultType=="text"){
        out <- cid3$results$data
    }
    knitr::engine_output(options, options$code, out,extra)
}
