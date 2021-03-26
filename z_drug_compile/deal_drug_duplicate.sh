#/usr/bin/bash

cur_date=$(date "+%Y-%m-%d")
base_p=/user/hive/warehouse/mining.db/jiangtao.yang/drug_compile_gcw
input_p=$base_p/drug_ana_${cur_date}.ori_gcw

hive -e "set spark.dynamicAllocation.maxExecutors=50;
        insert overwrite directory '${input_p}' row format delimited fields terminated by '\001'
select pid,regexp_replace(title, '[\r\n\t]', ''), retailprice from ( select *,row_number() over(partition by pid order by to_date(batch_time) desc) as rn from ods.ods_retail_jd_medicine_products)a where rn=1;"
if [ $? -ne 0 ]; then
    echo "getting review error!"
    exit 1
fi

output_p=$base_p/drug_ana_${cur_date}.drug_duplicate_v3.gcw
hadoop fs -rm -r -skipTrash $output_p

spark-submit \
    --num-executors 50 \
    --executor-memory 6g \
    --conf spark.sql.broadcastTimeout=3000 \
    drug_duplicate.spk.py 'online' $input_p $output_p
if [ $? -ne 0 ]; then
    echo "error!!! getting legal spu error! $"
    exit 1
fi

exit 0
