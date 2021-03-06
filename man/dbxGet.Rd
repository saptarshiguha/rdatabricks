% Generated by roxygen2 (4.1.0): do not edit by hand
% Please edit documentation in R/dbxClusterAdmin.R
\name{dbxGet}
\alias{dbxGet}
\title{Get information about  a cluster}
\usage{
dbxGet(cluster_id, token = options("databricks")[[1]]$token,
  instance = options("databricks")[[1]]$instance)
}
\arguments{
\item{cluster_id}{a string}
}
\value{
a big json
}
\description{
Get information about  a cluster
}
\details{
see https://docs.databricks.com/api/latest/clusters.html#get
}

