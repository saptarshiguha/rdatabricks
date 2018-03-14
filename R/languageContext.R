
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
##' @param s a commandid
##' @export
isCommandRunning <- function(s){
    return (identical(s$status,"Running"))
}


##' check if command is finished
##' @param s a command is
##' @export
isCommandFinished <- function(s){
    return (identical(s$status,"Finished"))
}


##' check if context is done
##' @param s a command id
##' @export
isCommandDone <- function(s){
    return (s$status %in% c("Cancelling","Finished","Error","Cancelled"))
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
  if(verbose>3) print(url)
  if(verbose)
      cat("Language context being created")
  pyctx<-POST(url,body=list(language=language, clusterId=clusterId)
    ,encode='form'
     ,authenticate(user,password))
  pcontent <- content(pyctx)
  pyctxId <- pcontent$id
  if(!is.null(pcontent$error))
      stop(sprintf("ctxmake: %s", pcontent$error))

  if(verbose>4) print(pcontent)
  if(verbose) cat("Query context being created")
  pyctx2<-POST(url,body=list(language=language, clusterId=clusterId)
              ,encode='form'
              ,authenticate(user,password))
  pcontent <- content(pyctx2)
  pyctxId2 <- pcontent$id
  if(verbose>4) print(pyctx2)
  if(!is.null(pcontent$error))
      stop(sprintf("ctxmake: %s", pcontent$error))

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

  allcontexts <- getOption("dbctxlist")
  if(is.null(allcontexts)) allcontexts <- list()
  allcontexts[[ pyctxId ]] <- list(ctx=pyctxId, q=pyctxId2)
  options(dbctxlist = allcontexts)
  pyctxId
}
  
##' Get Status of a Context>
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
##' @param ctx the context for the language (if null destroys all contexts)
##' @param instance is the instance of databricks 
##' @param clusterId is the clusterId you're working with
##' @param user your usename
##' @param password your password
##' @details see https://docs.databricks.com/api/1.2/index.html#execution-context
##' @return an id you have nothing to do with
##' @export 
dbxCtxDestroy <- function(ctx=NULL
                    ,instance=options("databricks")[[1]]$instance
                    ,clusterId=options("databricks")[[1]]$clusterId
                    ,user=options("databricks")[[1]]$user
                    ,password=options("databricks")[[1]]$password,verbose=0)
{
    .des <- function(ctx,instance, clusterId,user,password){
        checkOptions(instance, clusterId,user, password)
        url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/contexts/destroy",instance=instance)
        ctxDestroyCall<-POST(url,body=list(clusterId=clusterId,contextId=ctx)
                            ,encode='form'
                            ,authenticate(user,password))
        ctxDestroy <- content(ctxDestroyCall)
        allcontexts <- getOption("dbctxlist")
        allcontexts[[ ctx ]] <- NULL
        options(dbctxlist = allcontexts)
        ctxDestroy
    }
    if(!is.null(ctx)){
        .des(ctx,instance, clusterId, user,password)
    }else{
        allcontexts <- getOption("dbctxlist")
        for(a in allcontexts){
            if(verbose>2) print(sprintf("destroying %s, %s", a[[1]],a[[2]]))
            .des(a[[1]],instance, clusterId,user,password)
            .des(a[[2]],instance, clusterId,user,password)
        }
        options(querycontext=NULL)
        options(dbpycontext=NULL)
        options(dbrcontext=NULL)
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
##' @export 
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

##' @export
sendRaw <- function(code
                  , ctx = getOption("dbpycontext")
                  , instance =getOption("databricks")$instance
                  , clusterId = getOption("databricks")$clusterId
                  , user =  getOption("databricks")$user
                  , password =  getOption("databricks")$password
                  , verbose=0,wait=FALSE){
    url <- infuse("https://{{instance}}.cloud.databricks.com/api/1.2/commands/execute",instance=instance)
    if(verbose>=2) cat(code)
    commandUrl<-POST(url
                    ,body=list(language='python'
                              ,clusterId=clusterId
                              ,contextId=ctx
                              ,command=code)
                    ,encode='form'
                    ,authenticate(user,password))
    pc <- content(commandUrl)
    if( !is.null(pc$error))
        stop(sprintf("rdatabricks: %s\nYou might want to just call dbctx()"
                    ,content(commandUrl)$error))
    if(wait){
        while(TRUE){
            status <- dbxCmdStatus(pc$id,ctx,instance,clusterId,user,password)
            if(!isCommandDone(status)){ Sys.sleep(2); } else break
            }
    }
    return(pc)
}
