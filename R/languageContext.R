
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
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#execution-context
##' @return a context object
##' @export 
dbxCtxMake <- function(language='python',instance=options("databricks")[[1]]$instance
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
dbxCtxStatus <- function(ctx
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
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @export 
dbxRunCommand <- function(command, ctx,wait=0,language='python'
                    ,instance=options("databricks")[[1]]$instance
                    ,clusterId=options("databricks")[[1]]$clusterId
                    ,user=options("databricks")[[1]]$user
                    ,password=options("databricks")[[1]]$password)
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
  commandCtx <- content(commandUrl)$id
  if(wait>0){
    while(TRUE){
      status = dbxCmdStatus(commandCtx,ctx,instance,clusterId,user,password)
      if(isCommandRunning(status)) {cat("\n");return(status)} else {cat(".");sleep(wait)}
    }
  }else{
    commandCtx
  }
}

##' Command Status
##' @param cmdId commandId returned by runCommand
##' @param ctx the context for the language
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @export 
dbxCmdStatus <- function(cmdId
                    ,ctx
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
##' @param ctx the context for the language
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#command-execution
##' @return a commandId
##' @export 
dbxCmdCancel <- function(cmdId
                    ,ctx
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

