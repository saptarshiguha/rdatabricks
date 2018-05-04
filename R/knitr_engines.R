
replace.r.code <- function(code,andOutput,varEnv){
    ## replace code with variables
    while(TRUE){
        repl <- gregexpr("\\(__REPLACE__[a-zA-z0-9]+\\)",code)[[1]]
        repl.ml <- attr(repl,"match.length")
        i <- 1
        if(!(length(repl)==1 & repl[1]==-1)){
                var0 <- substring(code, repl[i],repl[i]+repl.ml[i]-1)
                var1 <- strsplit(var0,"__")[[1]][[3]]
                var1 <- substr(var1,1,nchar(var1)-1)
                theVar <- (get(var1,envir=varEnv))
                code <- gsub(var0, theVar, code,fixed=TRUE)
        }else break
    }
    if(andOutput)
        cat(sprintf("----Code Sent to Databricks Python Context----\n%s\n-----\n",code))
    code
}


getResults <- function(cid,verbose=FALSE,options,interactiveCall=TRUE){
    ## cid is returned from status
    if(identical(cid$results$resultType,"error")){
        ## Error
        cause <- cid$results$cause
        return(list(type='error', x=sprintf("%s\n",cause)))
    }else if(identical(cid$results$resultType,"image")){
        ## image
        system(sprintf("dbfs cp dbfs:/FileStore%s ./", cid$results$fileName,tempdir()))
        f <- sprintf("./%s",basename(cid$results$fileName)) ##tempdir was 1st param
        file.copy(f,"~/public_html/tmp/")
        if(verbose) message(sprinf("wrote %s to ",f))
        if(file.exists("~/imgcat") && interactive()){
            system(sprintf("~/imgcat %s",f))
        }
        if(TRUE || interactiveCall){
            extra <- sapply(f, function(f) knitr::knit_hooks$get("plot")(f, options))
            out <- cid$results$data
            return(list(type='image', x=sprintf("%s/%s",ifn(getOption("databricks")$imgdump,'NA'),basename(cid$results$fileName))
                        ,extra=extra,out=out))
        }
    }else if(identical(cid$results$resultType,"text")){
        out <- cid$results$data
        return(list(type='text', x=sprintf("%s\n",out)))
    }else if(identical(cid$status, "Running")){
        return(list(type='running', x=''))
    }
}

ifn <- function(s,f) if(is.null(s)) f else s
##' @export
dbxCancelAllJobs <- function(...){
    ## code is executed on the 'command' context
    options <- list(...)
    options$ctx <- getOption("querycontext")
    options$code <- "sc.cancelAllJobs()"
    options$showOutput <- FALSE
    options$showCode <- FALSE
    options$displayLog <- FALSE
    invisible(do.call(dbxExecuteCommand,options))
}


getSavedData <- function(t,dataloc,p,verbose){
    dbxSleepHack <- getOption("dbxSleepHack")
    if(!is.null(dbxSleepHack))
        Sys.sleep(dbxSleepHack) ##HACK
    system(sprintf("rm -rf %s/",t))
    gets <- sprintf("aws s3 sync %s/ %s/ > /dev/null 2>&1",dataloc,t)
    if(verbose) print(gets)
    system(gets);
    #print(sprintf("%s/%s_lastobject.json",t,p))
    #print(list.files(sprintf("%s/",t),full=TRUE))
    if(file.exists(sprintf("%s/%s_lastobject.feather",t,p))){
        require(feather)
        require(data.table)
        data.table(read_feather(sprintf("%s/%s_lastobject.feather",t,p)))
    }else if(file.exists(sprintf("%s/%s_lastobject.json",t,p))){
        require(rjson)
        #print(sprintf("%s/%s_lastobject.json",t,p))
        fromJSON(file=sprintf("%s/%s_lastobject.json",t,p))
    }
}
    
##' @export
dbxExecuteCommand <- function(...){
    options <- list(...)
    dbo <- getOption("databricks")
    ctx <- ifn(options$ctx,getOption("dbpycontext"))
    instance <- ifn(options$instance,dbo$instance)
    clusterId <- ifn(options$clusterId,dbo$clusterId)
    user <- ifn(options$user,dbo$user)
    password <- ifn(options$password,dbo$password)

    ## main options
    ## code
    ## showProg TRUE
    ## varEnv GlobalEnv
    ## stopOnError stop
    ## verbose 0
    ## displayLog TRUE
    ## autoSave FALSE
    ## interactiveCall TRUE
    ## showCode TRUE
    ## showOutput TRUE
    ## setJobGroup NULL
    
    code <- options$code
    rcode <- code
    if(is.null(code)) stop("provided code is missing!")

    checkOptions(code,ctx,instance, clusterId,user, password)
    ## show progress bars?
    showProg <- ifn(options$progress,FALSE)
    varEnv <- ifn(options$varEnv, .GlobalEnv)
    stopOnError <- ifn(options$stopOnError,TRUE)
    if(stopOnError)
        stopOnError <- function(e,...) { cat(list(...)$parseException); stop(e) }
    else
        stopOnError <- function(s) warning(s, immediate.=TRUE)

    verbose <- ifn(options$verbose,0)

    displayLog <- ifn(options$displayLog,TRUE)

    autoSave <- ifn(options$autoSave,FALSE)
    
    interactiveCall <- ifn(options$interactiveCall,TRUE)

    showCode <- ifn(options$showCode,TRUE)

    showOutput <- ifn(options$showOutput,TRUE)

    ## Almost every Code Sent to Python has a JobGroup associated with it
    ## Even if it's not spark code
    jobgroup <- ifn(options$setJobGroup,FALSE)
    if(jobgroup){
        jg  <- sprintf("sguha %s",digest(runif(1),algo='md5'))
        code <- sprintf("\n%s\n%s",sprintf("sc.setJobGroup('%s', 'Saptarshi Guha')",jg),code)
    }

    ## replace 'macro's in code
    code <- replace.r.code(code, showCode,varEnv)

    ## Show log output during code?
    sendRaw(code=sprintf("hdlr.clear()"),
            instance=instance,
            clusterId=clusterId,
            ctx=ctx,verbose=verbose,
            user=user,password=password,wait=TRUE)
    
    if(displayLog){
        s3location <- getOption("databricks")$log$location
        tfile <- tempfile(pattern='dbx')
        showLogs <- function(){
            currentlines <- ""
            function(){
                if(!is.null(s3location)){
                    oo <- sprintf("aws s3 cp s3://%s %s > /dev/null 2>&1", s3location, tfile)
                    tryCatch({
                        system(oo)
                        if(file.exists(tfile)){
                            newlines <- readLines(tfile)
                            extra <- setdiff(newlines, currentlines)
                            mm <- paste(extra, collapse="\n")
                            if(!mm=="")
                                message(mm)
                            currentlines <<- newlines
                        }
                    },error=function(e) stop(as.character(e)))
                }
            }
        }
        show.logger <- showLogs()
    }else show.logger <- function() {}

    ## code
    ## showProg TRUE
    ## varEnv GlobalEnv
    ## stopOnError stop
    ## verbose 0
    ## displayLog TRUE
    ## autoSave FALSE
    ## interactiveCall TRUE
    ## showCode TRUE
    ## showOutput TRUE
    ## setJobGroup NULL
    
    if(verbose>5){
        print(list(code=code,showProg=showProg,varEnv=varEnv, stopOnError=stopOnError, verbose=verbose,
                   displayLog=displayLog, autoSave=autoSave, interact=interactiveCall,showCode=showCode,
                   showOutput=showOutput, setJobGroup=options$setJobGroup))
    }
    
    ## Automatically save output assuming the end is DataFrame or Dict
    require(digest)
    tid <- digest(runif(1),algo="md5")
    f <- getOption("databricks")$log$dataKeyPrefix
    if(is.null(f) && (showProg || autoSave)){
        stop("getOption('databricks')$log$dataKeyPrefix missing,auto data download(needed by autoSave or Progress) wont work")
    }
    bucket <- getOption("databricks")$log$bucket
    datadir <- sprintf("%s/%s",f,tid)
    dataloc <- sprintf('s3://%s/%s', bucket, datadir)
    if(autoSave){
        code = sprintf("
___lastvalue = __exec_then_eval('''%s''')
__saveToS3(___lastvalue,\"%s\",\"%s\",\"p\")
___lastvalue
", code,bucket,datadir)
        if(verbose>5) print(code)
    }


    ## And now we have the code! Lets run this code
    ## if autoSave is TRUE ==> objects will be saved
    ## if displayLog is TRUE ==> logs will be shown
    ## if progress is TRUE ==> show progress bars (nuanced)
    commandCtx <- sendRaw(code=code, instance=instance,
                          clusterId=clusterId,
                          ctx=ctx,
                          user=user,password=password,
                          verbose=verbose)$id
    ## Get Status of Command before we get infos and progress
    status <- dbxCmdStatus(commandCtx,ctx,instance,clusterId,user,password)
    if (status$status=='Queued'){
        warning("There is job already running, this won't run till that finishes. Call dbxCancelAllJobs() or wait"
                ,immediate.=TRUE)
    }
    
    ## Since the command is not queue we should move to the end
    if(showProg){
        tf <- tempfile()
        require(progress)
        pb <- progress_bar$new(format = "Elapsed :elapsed Job: :job Stages: :ndone/:nstage Names :name [:bar] :percent eta: :eta"
                            ,  clear = FALSE ,total=100, width = 120)
    }
    while(TRUE){

        statusResult <- getResults(status, verbose=verbose, options=options,interactiveCall=interactiveCall)
        show.logger()
        if(statusResult$type=='error'){
            assign(".Last.dberr",list(status,statusResult),env=.GlobalEnv)
            str <- sprintf("%s\nPython Error\n",status$results$cause)
            if(grepl("\n(AnalysisException|ParseException): ",status$results$cause)){
                ff <- status$results$cause
                suffix <- regexec("ParseException: [u]*\"\\\\n(.*)\\\\n\"", ff)
                if(length(regmatches(ff,suffix)[[1]])==0)
                    suffix <- regexec("ParseException: [u]*\'\\\\n(.*)\\\\n\'", ff)
                if(length(regmatches(ff,suffix)[[1]])==0)
                    suffix <- regexec("AnalysisException: [u]*\"(.*)\\\\n\"", ff)
                str2 <- gsub("\\\\n","\n",sprintf("%s\n",regmatches(ff,suffix)[[1]][2]))
            }else str2=""
            stopOnError(sprintf("%s\n%s\nPython Error\n",str,str2))
            break
        }else if(isCommandRunning(status)){
            ## use the command context to query these since the main one has something running
            if(showProg){
                                        #if(!autoSave) stop("Progress monitoring of jobs requires autoSave=TRUE")
                on.exit({
                    if(exists("tf")) unlink(tf,rec=TRUE)
                })
                monitor = sprintf("
___lastvalue = __exec_then_eval('''
__getStuff(\'%s\')
''')
__saveToS3(___lastvalue,\'%s\',\'%s\',\'q\')
___lastvalue", jg,bucket,datadir)
                dbxExecuteCommand(code=monitor
                                 ,ctx=getOption("querycontext")
                                 ,displayLog=FALSE
                                 ,showOutput=FALSE
                                 ,showCode=FALSE)
                s <- getSavedData(tf,dataloc,"q",verbose)
                #assign("foo",s,env=.GlobalEnv)
                if(!is.null(s$data)){
                    s1 <- rbindlist(lapply(s$data, function(l){
                        as.data.table(l)
                    }))[, "jobid":=s$jobid,][order(id),][, c("jobid","id","name","ntasks","natasks","nctasks","nftasks"),with=FALSE]
                    s1 <- s1[, "progress":=round(nctasks/ntasks,2)][,'time':=s$t][,]
                    nprogress <- min(1,sum(s1$ntasks*s1$progress)/sum(s1$ntasks))
                    active <- s1[, sum(natasks>0)]
                    whichName <- s1[natasks>0, paste( unique(name),sep=" ",collapse="/")]
                    whichName <- sprintf("%s...",substr(whichName,1, min(30,nchar(whichName))))
                    if(!pb$finished && !is.na(nprogress))
                        pb$update(nprogress,tokens = list(job=s1$jobid[1],ndone=s1[,sum(nctasks==ntasks)],nstage = nrow(s1),name=whichName))
                }else{
                    if(!pb$finished)
                        pb$tick(1/1.0e6,token=list(job='',ndone=0, nstage=0, name=''))
                }
                Sys.sleep(1)
            }
        }else if(isCommandDone(status)){
            break
        }
        status <- dbxCmdStatus(commandCtx,ctx,instance,clusterId,user,password)
    }
    ## Wrap up: download any objects created
    assign(".Last.db",NULL,envi=.GlobalEnv)
    if(autoSave){
        tf2 <- tempfile()
        on.exit({ unlink(tf2,TRUE)},add=TRUE)
        res <- getSavedData(tf2,dataloc,"p",verbose)
        assign(".Last.db",res,envir=.GlobalEnv)
        if(!is.null(options$storein)){
            assign(as.character(options$storein),res,envir=.GlobalEnv)
        }
    }
    statusResult$givenOpts = options
    statusResult$code2 = code
    if(showOutput){
        cat("\n")
        cat(statusResult$x)
    }else{
        return(statusResult)
    }
}

databricksPythonEngine <- function(options){
    ## Do not run anything
    if(identical(options$eval,FALSE))
        return(knitr::engine_output(options, options$code, NULL,NULL))
    ctx <- options$ctx
    ## essentially no code! short-ciruit
    if (paste(options$code, sep = "", collapse = "") == "")
        return(knitr::engine_output(options, options$code, NULL, NULL))
    ## finally we have the code
    code <- paste(options$code, sep = "", collapse = "\n")
    options$code <- code
    extra <- NULL
    out <- NULL
    ox <- options
    options$showCode <- TRUE
    if(is.null(options$autoSave)) options$autoSave <- FALSE ##default
    if(!is.null(options$storein) && is.character(options$storein)){
        options$autoSave <- TRUE
        }
    options$showOutput <- FALSE
    options$progress <- TRUE
    options$setJobGroup <- TRUE
    cid3Results <- do.call(dbxExecuteCommand, options)
    options$engine <- 'python'
    if(TRUE || is.null(options$interactiveCall)){
        o <- ifn(cid3Results$x,"")
        if(identical('image',cid3Results$type)) o <- ""
        knitr::engine_output(options, options$code,o ,cid3Results$extra)
    }
}




