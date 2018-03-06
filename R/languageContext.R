
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
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#execution-context
##' @return a context object
##' @export 
dbxCtxMake <- function(language='python',instance=options("databricks")[[1]]$instance
                       ,wait=TRUE
                      ,clusterId=options("databricks")[[1]]$clusterId
                      ,user=options("databricks")[[1]]$user
                      ,password=options("databricks")[[1]]$password)
{
  checkOptions(instance, clusterId,user, password)
  url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/contexts/create",instance=instance)
  pyctx<-POST(url,body=list(language=language, clusterId=clusterId)
    ,encode='form'
    ,authenticate(user,password))
  pyctxId <- content(pyctx)$id
  loglocation <- NULL
  if(!is.null(f <- getOption("databricks")$log)){
      bucket <- f$bucket
      prefix <- f$prefix
      if(endsWith(bucket,"/")) bucket <- substr(bucket, 1,nchar(bucket)-1)
      if(endsWith(prefix,"/")) prefix <- substr(prefix, 1,nchar(prefix)-1)
      f$location <- sprintf("%s/%s/%s/logfile.txt", bucket, prefix,pyctxId) 
      o <- getOption("databricks")
      o$log <- f
      options(databricks=o)
  }
  if(wait){
      while(TRUE){
          ctxStats <- dbxCtxStatus(pyctxId)
          if(isContextRunning(ctxStats)) break
      }
      options(dbpycontext=pyctxId)
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
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @export 
dbxRunCommand <- function(command, ctx,wait=0,language='python'
                         ,poll.log=TRUE 
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
    cat(sprintf("Waiting for command: %s to finish\n", commandCtx))
    while(TRUE){
        status = dbxCmdStatus(commandCtx,ctx,instance,clusterId,user,password)
        if(poll.log){
            oo <- sprintf("aws s3 cp s3://%s %s > /dev/null 2>&1", s3location, tfile)
            system(oo)
            if(file.exists(tfile)){
                newlines <- readLines(tfile)
                extra <- setdiff(newlines, currentlines)
                message(paste(extra, collapse="\n"))
                currentlines <- newlines
            }
        }
        if(!isCommandRunning(status)) {cat(".\n");return(status)} else {cat(".");Sys.sleep(wait)}
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

