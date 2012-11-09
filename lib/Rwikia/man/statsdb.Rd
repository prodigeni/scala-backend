\name{statsdb}
\alias{statsdb}
\title{statsdb}
\description{Functions for accessing Wikia's data warehouse.}
\examples{
## Get a connection to the statsdb database
db <- statsdb()

## Retrieve the results of a query as a data.frame
user_dat <- dbGetQuery(db, "SELECT user_id FROM dimension_users LIMIT 25")

## A shortcut way that discards the database connection after each query
user_dat <- statsdb("SELECT user_id FROM dimension_users LIMIT 25")
}
\author{Jonathan March <jmarch@wikia-inc.com>}
