statsdb <- function(query = '') {
  conn <- dbConnect(MySQL(), user="statsdb", password="", dbname="statsdb", host="dw-s1")

  if (query == '')
    return(conn)
  else {
    results <- dbGetQuery(conn, query);
    dbDisconnect(conn);
    return(results)
  }
}

