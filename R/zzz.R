.onLoad <- function(lib, pkg) {
    #oldpyEngine <- (knitr:::knit_engines$get())$python
    #options(databricksOldPythonEgine = oldpyEngine)
    knitr:::knit_engines$set(pydbx = databricksPythonEngine)
    #knitr:::knit_engines$set(rdbx = databricksREngine)
    
}
