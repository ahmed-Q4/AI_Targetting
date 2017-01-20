# To read the model back in:
save.model.path <- "C:/Users/Administrator/Documents/R_workspaces/AI_Targetting/ModelResults/"
funds_ids <- dbGetQuery(con, "SELECT FACTSET_FUND_ID FROM fund_ids_us")
for(f in funds_ids) {
  fname <- paste0(save.model.path,f,".txt")
  model_fit <- unserialize(charToRaw(readChar(fname, file.info(fname)$size))) # To read the model back in:
}
