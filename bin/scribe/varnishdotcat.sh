#!/bin/sh

DIR=`dirname $0`;
/usr/bin/varnishlog  -c -I '__onedot' | perl $DIR/onedot_cat.pl
