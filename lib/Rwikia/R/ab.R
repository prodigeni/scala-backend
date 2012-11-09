ab_eval = function(source, a_id, b_id, percentile) {
    a_sample = head(sort(source$events[source$treatment_group_id == a_id]), n=percentile*length(source$events[source$treatment_group_id == a_id]));
    b_sample = head(sort(source$events[source$treatment_group_id == b_id]), n=percentile*length(source$events[source$treatment_group_id == b_id]));
    a_mean = mean(a_sample);
    b_mean = mean(b_sample);
    cat(paste(" Mean of A: ", round(a_mean,4), "\n"));
    cat(paste(" Mean of B: ", round(b_mean,4), "\n"));
    cat(paste("Change (%): ", round(100 * (b_mean - a_mean) / a_mean, 4), "%\n"));
    ttest_result  <- t.test(a_sample, b_sample);
    wilcox_result <- wilcox.test(a_sample, b_sample);
    cat(paste("wilcox.test p-value: ", round(wilcox_result$p.value, 4), "\n"));
    cat(paste("     t.test p-value: ", round(ttest_result$p.value,4), "\n"))
}
