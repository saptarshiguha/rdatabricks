##' Databricks Python Engine for Knitr
##'@export
databricksPythonEngine <- function(options){
    if(identical(options$eval,FALSE)) return(knitr::engine_output(options, options$code, NULL,NULL))
    if(is.null(getOption("dbpycontext"))){
        r <- dbxCtxMake(wait=TRUE)
    }
    if(is.null(options$ctx))
        ctx <- getOption("dbpycontext")
    else
        ctx <- options$ctx
    if (paste(options$code, sep = "", collapse = "") == "")
        return(knitr::engine_output(options, options$code, NULL, NULL))
    code <- paste(options$code, sep = "", collapse = "\n")
    if(!is.null(getOption("databricks")$currentCommand)) {
        pc <- dbxCmdStatus()
        if(!identical(pc$status,"Finished")){
            message(sprintf("Already running command( %s ), with status: %s. You can kill it with dbxCmdCancel('%s') ", pc$id,pc$status,pc$id))
        }
    }
    extra = NULL
    out <- NULL
    wait <- options$wait
    if(is.null(wait)) wait <- 3
    ## set jobgroup
    ## replace code with variables
    repl <- gregexpr("\\(__REPLACE__[a-zA-z0-9]+\\)",code)[[1]]
    repl.ml <- attr(repl,"match.length")
    if(!(length(repl)==1 & repl==-1)){
        for(i in seq_along(repl)){
            var0 <- substring(code, repl[i],repl[i]+repl.ml[i]-1)
            var1 <- strsplit(var0,"__")[[1]][[3]]
            var1 <- substr(var1,1,nchar(var1)-1)
            theVar <- deparse(get(var1,envir=if(is.null(options$dbenv)) .GlobalEnv else options$dbenv))
            code <- gsub(var0, theVar, code,fixed=TRUE)
            }
    }
    if(!identical(options$showme,FALSE)) cat(sprintf("----Code Sent to Databricks Python Context----\n\n%s\n\n-----\n",code))
    jg <- getOption("currentJobGroup")
    if(TRUE || is.null(jg)){
        jg  <- sprintf("sguha %s",digest(runif(1),algo='md5'))
        options(currentJobGroup = jg)
    }
    code <- sprintf("sc.setJobGroup('%s', 'SGuhas work')
__jg = '%s'
%s",jg,jg,code)
    require(digest)
    tid <- digest(runif(1),algo="md5")
    if(is.null(f <- getOption("databricks")$log$dataKeyPrefix)){
        warning("log$data missing,auto data download wont work")
    }else{

        if(identical(options$autoDownloadObject,TRUE)){
            bk <- getOption("databricks")$log$bucket
            datadir <- sprintf("%s/%s",f,tid)
            ## Modify Code to write output to folder
            dloc <- sprintf('s3://%s/%s', bk, datadir)
            if(!identical(options$showOutput,FALSE)) message(sprintf("Useful objects for this run (if any) found at %s", dloc))
            code = sprintf("
___lastvalue = exec_then_eval('''
%s
''')
if ___lastvalue is not None:
 _saveToS3(___lastvalue,'%s','%s')
___lastvalue
", code,bk,datadir)
        }

    }
    if(options$eval){
        cid3 <- dbxRunCommand(code,ctx=ctx,language='python',wait=wait,quiet=identical(options$showOutput,FALSE),
                              progress = if(is.null(options$progress)) FALSE else TRUE,
                              poll.log=if(is.null(options$poll)) TRUE else as.logical(options$poll))
        if(!is.null(getOption("databricks")$debug)
           && getOption("databricks")$debug>0){
            print(cid3)
        }
        if(is.character(cid3)) return(cid3)
        if(!is.null(cid3$status) && cid3$status=="Queued"){
            warning(sprintf("Command with id: %s is queued,waiting", cid3$id))
            while(TRUE){
                ss <- dbxCmdStatus(cid3$id)
                if(identical("Queued",ss$status)) {Sys.sleep(wait);cat(".")} else {cat("\n");break}
            }
        }
        if(identical(cid3$results$resultType,"error")){
            (out <- cid3$results$cause)
            if(identical(options$stopOnError,TRUE)){
                cat(sprintf("%s\n",out))
                stop("Error In Python Code")
            }
        }else if(identical(cid3$results$resultType,"image")){
            system(sprintf("dbfs cp dbfs:/FileStore%s %s/", cid3$results$fileName,tempdir()))
            f <- sprintf("%s/%s",tempdir(),basename(cid3$results$fileName))
            if(file.exists("~/imgcat") && interactive()){
                file.copy(f,"~/public_html/tmp/")
                message(sprintf("viewing %s ", basename(f)))
                system(sprintf("~/imgcat %s",f))
            }
            if(is.null(options$fromEmacs))
                extra <- sapply(f, function(f) knitr::knit_hooks$get("plot")(f, options))
            out <- cid3$results$data
            if(is.null(out)) out <- ""
        }else if(identical(cid3$results$resultType,"text")){
            out <- cid3$results$data
        }
        if(identical(options$autoDownloadObject,TRUE)){
            system(sprintf("rm -rf /tmp/jaxir ; aws s3 sync %s/ /tmp/jaxir &>/dev/null",dloc))
            if(file.exists("/tmp/jaxir/lastobject.feather")){
                require('feather');require(data.table)
                assign(".Last.dbx",data.table(read_feather("/tmp/jaxir/lastobject.feather")),envi=.GlobalEnv)
            }else if(file.exists("/tmp/jaxir/lastobject.json")){
                require(rjson)
                assign(".Last.dbx",fromJSON(file="/tmp/jaxir/lastobject.json"),envir=.GlobalEnv)
            }
        }
    }
    options$engine='python'
    if(is.null(options$fromEmacs))
        knitr::engine_output(options, options$code, out,extra)
    else{
        if(!identical(options$showOutput,FALSE)) cat(sprintf("\n----Output----\n\n %s\n\n----\n",out))
    }
}

## ##' Databricks R Engine Knitr
## ##'@export
## databricksREngine <- function(options){
##     ## if(!(!is.null(options$dbx) && options$dbx==TRUE)){
##     ##     return(getOption("databricksOldPythonEgine")(options))
##     ## }
##     if(is.null(getOption("dbpycontext"))){
##         r <- dbxCtxMake()
##         while(TRUE){
##             ctxStats <- dbxCtxStatus(r)
##             if(isContextRunning(ctxStats)) break
##         }
##         options(dbpycontext=r)
##     }
##     ctx <- getOption("dbpycontext")
##     if (paste(options$code, sep = "", collapse = "") == "")
##         return(knitr::engine_output(options, options$code, NULL, NULL))
##     code <- paste(options$code, sep = "", collapse = "\n")
##     extra = NULL
##     out <- NULL
##     if(options$eval){
##         cid3 <- dbxRunCommand(code,ctx=ctx,language='r',wait=3)
##         if(!is.null(getOption("databricks")$debug)
##            && getOption("databricks")$debug>0){
##             print(cid3)
##         }
##     if(cid3$results$resultType=="error"){
##         (out <- cid3$results$cause)
##     }else if(cid3$results$resultType=="image"){
##         system(sprintf("dbfs cp dbfs:/FileStore%s %s/", cid3$results$fileName,tempdir()))
##         f <- sprintf("%s/%s",tempdir(),basename(cid3$results$fileName))
##         extra <- sapply(f, function(f) knitr::knit_hooks$get("plot")(f, options))
##     out <- cid3$results$data
##         if(is.null(out)) out <- ""
##     }else if(cid3$results$resultType=="text"){
##         out <- cid3$results$data
##     }
##     }
##     options$engine='R'
##     knitr::engine_output(options, options$code, out,extra)
## }
