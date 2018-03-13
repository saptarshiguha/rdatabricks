
checkOptions <- function(...){
  parms <- list(...)
  if(any(unlist(lapply(parms, function(k) is.null(k) || length(k)==0))))
    stop("One or more passed parameters is NULL or of length zero")
}

##' check if context is running
##' @param ctx a context
##' @export
isContextRunning <- function(s){
    return (identical(s$status,"Running"))
}


##' check if context is running
##' @param ctx a context
##' @export
isCommandRunning <- function(s){
    return (identical(s$status,"Running"))
}

##' Create a Remote Language Context
##' @param the language context
##' @param instance is the instance of databricks
##' @param wait should i wait for context to start?
##' @param private should it modify options?
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#execution-context
##' @return a context object
##' @export 
dbxCtxMake <- function(language='python',instance=options("databricks")[[1]]$instance
                      ,wait=TRUE,verbose=FALSE
                      ,clusterId=options("databricks")[[1]]$clusterId
                      ,user=options("databricks")[[1]]$user
                      ,password=options("databricks")[[1]]$password)
{
  checkOptions(instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/contexts/create",instance=instance)
  if(verbose)
      cat("Language context being created")
  pyctx<-POST(url,body=list(language=language, clusterId=clusterId)
    ,encode='form'
    ,authenticate(user,password))
  pyctxId <- content(pyctx)$id

  if(verbose) cat("Query context being created")
  pyctx2<-POST(url,body=list(language=language, clusterId=clusterId)
              ,encode='form'
              ,authenticate(user,password))
  pyctxId2 <- content(pyctx2)$id

  loglocation <- NULL
  if(!is.null(f <- getOption("databricks")$log)){
      bucket <- f$bucket
      prefix <- f$prefix
      if(endsWith(bucket,"/")) bucket <- substr(bucket, 1,nchar(bucket)-1)
      if(endsWith(prefix,"/")) prefix <- substr(prefix, 1,nchar(prefix)-1)
      f$location <- sprintf("%s/%s/%s/logfile.txt", bucket, prefix,pyctxId)
      f$dataKeyPrefix <- sprintf("%s/%s/data", prefix,pyctxId) 
      o <- getOption("databricks")
      o$log <- f
      options(databricks=o)
  }
  if(wait){
      while(TRUE){
          ctxStats <- dbxCtxStatus(pyctxId)
          ctxStats2 <- dbxCtxStatus(pyctxId2)
          if(isContextRunning(ctxStats) && isContextRunning(ctxStats2)) break
      }
      if(language=='python') options(dbpycontext=pyctxId)
      if(language %in% c('R','r')) options(dbrcontext=pyctxId)
      options(querycontext=pyctxId2)
  }
  pyctxId
}
  
##' Get Status of a Context
##' @param ctx the context for the language
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#execution-context
##' @return a context object
##' @export 
dbxCtxStatus <- function(ctx=getOption("dbpycontext")
                    ,instance=options("databricks")[[1]]$instance
                    ,clusterId=options("databricks")[[1]]$clusterId
                    ,user=options("databricks")[[1]]$user
                    ,password=options("databricks")[[1]]$password)
{
  checkOptions(instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/contexts/status",instance=instance)
  ctxStatusCall<-GET(url,query=list(clusterId=clusterId,contextId=ctx)
            ,authenticate(user,password))
  content(ctxStatusCall)
}

##' Destroy. a Context
##' @param ctx the context for the language
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#execution-context
##' @return an id you have nothing to do with
##' @export 
dbxCtxDestroy <- function(ctx
                    ,instance=options("databricks")[[1]]$instance
                    ,clusterId=options("databricks")[[1]]$clusterId
                    ,user=options("databricks")[[1]]$user
                    ,password=options("databricks")[[1]]$password)
{
 checkOptions(instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/contexts/destroy",instance=instance)
  ctxDestroyCall<-POST(url,body=list(clusterId=clusterId,contextId=ctx)
                     ,encode='form'
                     ,authenticate(user,password))
  ctxDestroy <- content(ctxDestroyCall)
}


##' Command execute
##' @param command a string command to be executed
##' @param ctx the context for the language
##' @param wait if non zero, will wait till command is finnished. wait is seconds polling
##' @param language is the language of the command
##' @param poll.log TRUE if we need to poll the s3 log file for output from remote code
##' @param quiet supress all aoutput
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @export 
dbxRunCommand <- function(command, ctx,wait=0,language='python'
                         ,poll.log=TRUE ,quiet=FALSE,progress=FALSE
                         ,instance=options("databricks")[[1]]$instance
                         ,clusterId=options("databricks")[[1]]$clusterId
                         ,user=options("databricks")[[1]]$user
                         ,password=options("databricks")[[1]]$password
                          )
{
  checkOptions(command,ctx,instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/commands/execute",instance=instance)
  commandUrl<-POST(url
            ,body=list(language=language
                       ,clusterId=clusterId
                       ,contextId=ctx
                       ,command=command)
            ,encode='form'
             ,authenticate(user,password))
  if( !is.null(content(commandUrl)$error)) stop(sprintf("rdatabricks: %s\nYou might want to delete the dbpycontext option",content(commandUrl)$error))
  commandCtx <- content(commandUrl)$id
  dbo <- getOption("databricks")
  dbo$currentCommand <- commandCtx
  options(databricks=dbo)
  if(poll.log){
      s3location <- getOption("databricks")$log$location
      tfile <- tempfile(pattern='dbx')
  }
  currentlines <- ""
  if(wait>0){
      if(!quiet) cat(sprintf("Waiting for command: %s to finish\n", commandCtx))
    while(TRUE){
        status = dbxCmdStatus(commandCtx,ctx,instance,clusterId,user,password)
        if(poll.log && !is.null(s3location)){
            oo <- sprintf("aws s3 cp s3://%s %s > /dev/null 2>&1", s3location, tfile)
            tryCatch({
                system(oo)
                if(file.exists(tfile)){
                    newlines <- readLines(tfile)
                    extra <- setdiff(newlines, currentlines)
                    message(paste(extra, collapse="\n"))
                    currentlines <- newlines
                }
            },error=function(e) stop(as.character(e)))
        }
        if(progress){
            ## create a separate language context to query this command
            jg <- getOption('currentJobGroup')
            qctx <- getOption('querycontext')
            if(is.null(jg)) stop("NO JOB GROUP!")
            cc <- sprintf("
def getStuff(oo):
    from time import gmtime, strftime
    v=sc.statusTracker().getJobIdsForGroup(oo)
    v = filter(lambda x: sc.statusTracker().getJobInfo(x).status=='RUNNING',v)
    if len(v)>0:
        v2=sc.statusTracker().getJobInfo(v[0])
        s=list(v2.stageIds)
    else:
        v2=None
        s=None
    def parseStage(s):
        temp = {'id':0, 'name':'NA', 'ntasks':0, 'natasks':0, 'nctasks':0, 'nftasks':0}
        temp['id'] = s.stageId
        temp['name'] = s.name
        temp['ntasks'] = s.numTasks
        temp['natasks'] = s.numActiveTasks
        temp['nctasks'] = s.numCompletedTasks
        temp['nftasks'] = s.numFailedTasks
        return temp
    if s is not None:
        f = [ parseStage(sc.statusTracker().getStageInfo(a)) for a in s]
        return {'jobid': v[0], 'data':f,'t':strftime('%%H:%%M:%%S', gmtime())}
    else:
        return {'t':strftime('%%H:%%M:%%S UTC', gmtime())}

getStuff('%s')
", jg)
            require(progress)
            print("progerss is ture running different query")
            pb <- progress_bar$new( format = "Progress(:stages stages, :acts active, :names)  [:bar] :percent  eta: :eta", total = 100, clear = FALSE)
            invisible(pb$tick(0,tokens = list(stages=0, names='')))
            weDidNotSeeData <- 0;ranOnce <- FALSE
            while(TRUE && weDidNotSeeData<=5){
                ##dbx(cc,showme=FALSE,showOutput=FALSE,ctx=qctx)
                s <- list() #.Last.dbx
                ## We wait for 5 seconds before aborting (either long running puthon comp or empty array)
                ## keep in mind the adrray could be empty for a few seconds before pyspark job begins
                ## but it' shouldnt be empty for too long.
                if(!is.null(s$data)){
                    weDidNotSeeData<- 0
                    ranOnce <- TRUE
                    s1 <- rbindlist(lapply(s$data, function(l){
                        as.data.table(l)
                    }))[, "jobid":=s$jobid,][order(id),][, c("jobid","id","name","ntasks","natasks","nctasks","nftasks"),with=FALSE]
                    s1 <- s1[, "progress":=round(nctasks/ntasks,2)][,'time':=s$t][,]
                    progress <- sum(s1$ntasks*s1$progress)/sum(s1$ntasks)
                    active <- s1[, sum(natasks>0)]
                    whichName <- s1[natasks>0, paste( unique(name),sep=" ",collapse="/")]
                    pb$update(progress,tokens = list(stages = nrow(s1), acts=active,names=whichName))
                    Sys.sleep(3)
                } else{
                    weDidNotSeeData <- weDidNotSeeData+1
                    if(ranOnce) break
                    Sys.sleep(1)
                }
                
            }
        }else{
            if(!isCommandRunning(status)) {
                if(!quiet) cat(".\n");
                return(status)
            } else {
                if(!quiet) cat(".");
                Sys.sleep(wait)
            }
        }
    }
  }else{
      commandCtx
  }
}

##' Command Status
##' @param cmdId commandId returned by runCommand (by default, the current command)
##' @param ctx the context for the language (current python context)
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @
##' export 
dbxCmdStatus <- function(cmdId=getOption("databricks")$currentCommand
                    ,ctx=getOption("dbpycontext")
                    ,instance=options("databricks")[[1]]$instance
                    ,clusterId=options("databricks")[[1]]$clusterId
                    ,user=options("databricks")[[1]]$user
                    ,password=options("databricks")[[1]]$password)
{
  checkOptions(cmdId,ctx,instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/commands/status",instance=instance)
  cmdStatusCall<-GET(url,query=list(clusterId=clusterId
                                  ,contextId=ctx
                                  ,commandId=cmdId)
                   ,authenticate(user,password))
  (cmdStatus <- content(cmdStatusCall))
  return(cmdStatus)
}

##' Command Cancel
##' @param cmdId commandId returned by runCommand
##' @param ctx the context for the language (default is the current running python context)
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @export 
dbxCmdCancel <- function(cmdId=getOption("databricks")$currentCommand
                    ,ctx=getOption("dbpycontext")
                    ,instance=options("databricks")[[1]]$instance
                    ,clusterId=options("databricks")[[1]]$clusterId
                    ,user=options("databricks")[[1]]$user
                    ,password=options("databricks")[[1]]$password)
{
  checkOptions(cmdId,ctx,instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/commands/cancel"
              ,instance=instance)
  cancelUrl<-POST(url
                 ,body=list(clusterId=clusterId
                            ,contextId=ctx
                            ,commandId=cmdId)
                 ,encode='form'
                 ,authenticate(user,password))
  (cancelCtx <- content(cancelUrl))
  return(cancelCtx)
}

