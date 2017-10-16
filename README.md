# rdatabricks
An R Package that wraps the Databricks REST API. You can find the API at https://docs.databricks.com/api/index.html. This package is designed assuming you're a paying customer of DataBricks. 

The idea is to implement the cluster admin API (using their v2.0 API) and the command execution API (v1.2)

This package also contains engine for knitr so you can run RMarkdown documents
with codeblocks that call code that will be run on remote clusters.


