% Generated by roxygen2 (4.1.0): do not edit by hand
% Please edit documentation in R/languageContext.R
\name{dbxCtxDestroy}
\alias{dbxCtxDestroy}
\title{Destroy. a Context}
\usage{
dbxCtxDestroy(ctx, instance = options("databricks")[[1]]$instance,
  clusterId = options("databricks")[[1]]$clusterId,
  user = options("databricks")[[1]]$user,
  password = options("databricks")[[1]]$password)
}
\arguments{
\item{ctx}{the context for the language (if null destroys all contexts)}

\item{instance}{is the instance of databricks}

\item{clusterId}{is the clusterId you're working with}

\item{user}{your usename}

\item{password}{your password}
}
\value{
an id you have nothing to do with
}
\description{
Destroy. a Context
}
\details{
see https://docs.databricks.com/api/1.2/index.html#execution-context
}

