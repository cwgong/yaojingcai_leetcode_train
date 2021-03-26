#!/usr/bin/bash

stop_word_p="/user/hive/warehouse/mining.db/stop_word.txt"
output_p="/user/hive/warehouse/mining.db/hot_word_stat"

hadoop fs -rm -r -skipTrash $output_p
spark-submit \
    --num-executors 15 \
    --executor-memory 4g \
    --conf spark.sql.broadcastTimeout=3000 \
    hot_words_deal.py $output_p $stop_word_p

if [ $? -ne 0 ]; then
   echo "error"
   exit -1
fi

exit 0
