while true
do
    for i in `ls | grep csv$`
    do
	let wordcount=`cat $i | wc -l`
	if [ "$wordcount" -gt "0" ]; then
	    cat $i | awk 'BEGIN { FS = "\t"} ; { if ($1 && $2 && $3 && $4) urls[$1 "\t" $2 "\t" $3] = urls[$1 "\t" $2 "\t" $3] " | "  $4 } END { for (i in urls) { print i "\t" urls[i] }  };'  > $i.grouped
	    echo -e `date -d @\`date +%s\`` "\tGrouped $i";
	    perl /usr/wikia/backend/bin/backlinkdb-processcsv.pl $i.grouped
	    if [ $? -eq 0 ]; then
		echo -e `date -d @\`date +%s\`` "\tProcessed $i.grouped"
		mv $i processed
		mv $i.grouped processed
		gzip -f processed/$i
		gzip -f processed/$i.grouped
	    fi
	fi
	sleep 5
    done
done