#/usr/bin/bash

cur_date=$(date "+%Y-%m-%d")
base_p=/user/hive/warehouse/mining.db/jiangtao.yang/drug_compile_gcw
input_p=$base_p/drug_ana_${cur_date}.ori_gcw

hive -e "set spark.dynamicAllocation.maxExecutors=50;
        insert overwrite directory '${input_p}' row format delimited fields terminated by '\001'
select b.uu_id,b.drug_name,b.ori_id_set,a.ori_sku,a.ori_title,a.target_sku,a.target_title,a.similar_score,a.price from dwd.dwd_yaojingcai_price_attach_v2 a left join dwd.dwd_yaojingcai_drug_duplicate b on a.ori_sku = b.drug_name"
if [ $? -ne 0 ]; then
    echo "getting review error!"
    exit 1
fi

output_p=$base_p/drug_ana_${cur_date}.drug_result_v7.gcw
hadoop fs -rm -r -skipTrash $output_p

spark-submit \
    --num-executors 50 \
    --executor-memory 6g \
    --conf spark.sql.broadcastTimeout=3000 \
    drug_deal_compile_v3.spk.py 'online' $input_p $output_p
if [ $? -ne 0 ]; then
    echo "error!!! getting legal spu error! $"
    exit 1
fi

exit 0
