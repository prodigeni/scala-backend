wam_plot <- function(x,y, title, color="blue", invert=TRUE, ylabel="Rank") {
  if (invert) {
      plot(x, y, type="l", main=title, xlab="", ylab=ylabel, col=color, lwd=2, ylim=rev(range(y)))
  } else {
      plot(x, y, type="l", main=title, xlab="", ylab=ylabel, col=color, lwd=2)
  }
}

wam_trend <- function(domain) {
  wiki <- statsdb(paste("SELECT * FROM dimension_wikis w WHERE w.domain = '", domain, "'", sep=""));
  dat  <- statsdb(paste("SELECT DATE(pt.time_id) AS time_id, IFNULL(wam.wam_rank,5000) AS wam_rank, IFNULL(wam.wam,0) AS wam, pageviews_28day_rank, edits_28day_rank, editors_28day_rank, growth_4day_rank, growth_14day_rank, FLOOR(LOG(4.5, pageviews_8_to_5)) AS growth_4day_tier FROM statsdb_etl.etl_period_times pt JOIN fact_wam_scores wam ON wam.wiki_id = ", wiki$wiki_id, " AND wam.time_id = pt.time_id WHERE pt.period_id = 1 AND pt.time_id BETWEEN '2012-01-01' AND DATE_SUB(DATE(now()), INTERVAL 2 DAY) ORDER BY pt.time_id", sep=""));
  pv   <- statsdb(paste("SELECT DATE(pt.time_id) AS time_id, IFNULL(pv.pageviews,0) AS pageviews FROM statsdb_etl.etl_period_times pt LEFT JOIN rollup_wiki_pageviews pv ON pv.period_id = 1 AND pv.time_id = pt.time_id AND pv.wiki_id = ", wiki$wiki_id, " WHERE pt.period_id = 1 AND pt.time_id BETWEEN '2012-01-01' AND DATE_SUB(DATE(now()), INTERVAL 2 DAY) ORDER BY pt.time_id", sep=""));

  dom  <- gsub('.wikia.com', '', wiki$domain);

  par(mfrow=c(4,2))
  wam_plot(as.POSIXlt(dat$time_id), dat$wam_rank,             paste("WAM Rank: ", dom, sep=""), "red");
  wam_plot(as.POSIXlt(dat$time_id), dat$wam,                  paste("WAM: ", dom, sep=""), "red", FALSE, ylabel="Score");
  wam_plot(as.POSIXlt(pv$time_id),  pv$pageviews,             paste("Pageviews: ", dom, sep=""), invert=FALSE, ylabel="Pageviews");
  wam_plot(as.POSIXlt(dat$time_id), dat$pageviews_28day_rank, paste("Pageviews Rank (28 Day): ", dom, sep=""));
  wam_plot(as.POSIXlt(dat$time_id), dat$edits_28day_rank,     paste("Edits Rank (28 Day): ", dom, sep=""));
  wam_plot(as.POSIXlt(dat$time_id), dat$editors_28day_rank,   paste("Editors Rank (28 Day): ", dom, sep=""));
  wam_plot(as.POSIXlt(dat$time_id), dat$growth_4day_rank,     paste("Growth Rank (4 Day): ", dom, sep=""));
  wam_plot(as.POSIXlt(dat$time_id), dat$growth_14day_rank,    paste("Growth Rank (14 Day): ", dom, sep=""));
  # wam_plot(as.POSIXlt(dat$time_id), dat$growth_4day_tier,     paste("Growth Rank (4 Day): ", dom, sep=""));
  # data.frame(time_id = dat$time_id, wam = dat$wam, wam_rank = dat$wam_rank, pageviews = pv$pageviews)
  data.frame("Min WAM Rank" = min(dat$wam_rank), "Max WAM Rank" = max(dat$wam_rank))
}

