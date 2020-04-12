#!/usr/bin/env bash

source 
export queue=root.online.default

pre_1day=`date +%Y-%m-%d -d "1 day ago"`
pre_30day=`date +%Y-%m-%d -d "60 day ago"`
HQL_Header="use recall_db;
set mapred.job.name = recall_pool_detail_"$pre_1day";
set mapreduce.job.queuename=root.online.default;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 25610001000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=100000;
set hive.mapred.supports.subdirectories=true;
set mapreduce.input.fileinputformat.input.dir.recursive=true;
add jar /data/recall/udf/nexr-hive-udf-0.2-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION row_number AS 'com.nexr.platform.hive.udf.GenericUDFRowNumber';
"

HQL=$HQL_Header"
CREATE TABLE if not exists recall_newdoc_train_data (docid string,sourcename string,category string,attention_interest string,tagmap string)
PARTITIONED BY (skipType string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
insert OVERWRITE table recall_newdoc_train_data PARTITION (skipType)
select b.docid,b.sourcename,b.category,b.attention_interest,b.tagmap,b.skipType from
(select docid,sourcename,category,attention_interest,tagmap,if(skipType='','doc',skiptype) skiptype from
feature_article_all_inc
where bdms_sys_pdate>='$pre_30day' and belong='Toutiao' and skiptype in ('','video')
group by docid,sourcename,category,attention_interest,tagmap,skipType)b
"
echo $HQL
hive -e "$HQL" &

wait

cd fm_recall_data
rm train_data*
${HADOOP_HOME}/bin/hadoop fs -cat /user/datacenter/warehouse/recall_db.db/recall_newdoc_train_data/skiptype={doc,video}/* >wuliao_data
python parse_fm_train_data.py wuliao_data train_data

${HADOOP_HOME}/bin/hadoop fs -rm train_data /user/datacenter/recall/shitou/train_data
${HADOOP_HOME}/bin/hadoop fs -put train_data /user/datacenter/recall/shitou
cd ../

hour=`date +%Y%m%d -d "1 day ago"`
day=`date +%Y%m%d -d "1 day ago"`
pre1_day=`date +%Y%m%d -d "2 day ago"`
day_format=`date +%Y-%m-%d -d "1 day ago"`

sdkDir="/user/portal/datastream/portal/rec/newsappSDKETL/${day_format}/*/*"
docFeatureDir="/user/datacenter/recall/shitou"


engineDir="/datastream/portal/exposure/nginx/${day_format}/*"
path_sufix="/user/datacenter/recall/fm_feature/"
${HADOOP_HOME}/bin/hadoop fs -rm -r $path_sufix"train_data/"$hour
cmd="$SPARK_HOME/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --num-executors 200 \
    --executor-memory 6144m \
    --executor-cores 1 \
    --driver-memory 3G \
    --conf spark.driver.maxResultSize=3g \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    --conf spark.yarn.driver.memoryOverhead=3072 \
    --conf spark.yarn.am.memoryOverhead=3072 \
    --conf spark.default.parallelism=1000 \
    --conf spark.shuffle.io.maxRetries=20 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.network.timeout=600s \
    --conf spark.rpc.lookupTimeout=600s \
    --conf spark.rpc.askTimeout=600s \
    --queue $queue \
    --conf spark.executor.heartbeatInterval=20s \
    --class com.netease.fm_feature_nodocid.FM_Feature_Train \
    --packages com.github.scopt:scopt_2.11:3.7.0,org.jblas:jblas:1.2.4 \
    --jars /home/appops/jiande/hadoop-2.7.3/share/hadoop/common/lib/hadoop-lzo-0.4.20-SNAPSHOT.jar \
    --repositories http://maven.aliyun.com/nexus/content/groups/public/ \
    fm_feature_recall-1.0-SNAPSHOT.jar \
    --sdkDir ${sdkDir} \
    --docFeatureDir ${docFeatureDir} \
    --engineDir ${engineDir} \
    --outputDir ${path_sufix}train_data/${hour} \
    --partitionNum 100"
echo '------------------------------>'
echo $cmd
$cmd &

cd model/
${HADOOP_HOME}/bin/hadoop fs -cat /user/datacenter/recall/fm_feature/train_data/$hour/doc/*| /data/recall/tools/ftrlFM/bin/fm_train_io_parallelly -core 39 -dim 1,1,64 -w_alpha 0.27 -w_beta 1.0 -w_l1 0.01 -w_l2 0.1 -v_alpha 0.25 -v_beta 1.0 -v_l1 0.0 -v_l2 0.1 -m model.${day} -im model.${pre1_day}


model_cnt=`find ./ -name "model.${day}*"|xargs du -ck|grep total|awk 'BEGIN{sum=0} {sum+=$1}END{print sum}'`
if [ $model_cnt -ge 40000000 ]; then
  rm -rf model.${pre1_day}*
else
  echo $model_cnt | mail -s 'fm_feature_model_doc_day_train_error' 
  rm -rf model.${day}*
  for files in `ls model.${pre1_day}*`
  do
    mv $files `echo ${files}|sed 's/'${pre1_day}'/'${day}'/1'`
  done
fi

model_path_sufix=$path_sufix"model_data/"
${HADOOP_HOME}/bin/hadoop fs -rm -r $model_path_sufix$hour
${HADOOP_HOME}/bin/hadoop fs -mkdir $model_path_sufix$hour
${HADOOP_HOME}/bin/hadoop fs -put model.${day} $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_0 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_1 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_2 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_3 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_4 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_5 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_6 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_7 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_8 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_9 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_10 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_11 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_12 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_13 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_14 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_15 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_16 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_17 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_18 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_19 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_20 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_21 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_22 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_23 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_24 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_25 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_26 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_27 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_28 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_29 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_30 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_31 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_32 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_33 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_34 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_35 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_36 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_37 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_38 $model_path_sufix$hour &
wait
cd ../

# 5.生成缩减后的模型
user_path_sufix="/user/datacenter/warehouse/recall_db.db/recall_pool_user_hour_detail/hour=*/*"
dim=64
cmd="$SPARK_HOME/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --num-executors 100 \
    --executor-memory 3072m \
    --executor-cores 1 \
    --driver-memory 3G \
    --name article_mf-model-Shrink__"$day" \
    --conf spark.driver.maxResultSize=3g \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    --conf spark.yarn.driver.memoryOverhead=3072 \
    --conf spark.yarn.am.memoryOverhead=3072 \
    --conf spark.default.parallelism=1000 \
    --conf spark.shuffle.io.maxRetries=20 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.network.timeout=600s \
    --conf spark.rpc.lookupTimeout=600s \
    --conf spark.rpc.askTimeout=600s \
    --queue $queue \
    --conf spark.executor.heartbeatInterval=20s \
    --class com.netease.fm_feature_nodocid.model_Shrink \
    fm_feature_recall-1.0-SNAPSHOT.jar \
    $model_path_sufix \
    $user_path_sufix \
    $day \
    $dim"
echo '------------------------------>'
echo $cmd
$cmd

cd model/

model_cnt=`${HADOOP_HOME}/bin/hadoop fs -du ${model_path_sufix}"vaild_"${day}"/part-00000"|cut -d ' ' -f 1 `
if [ $model_cnt -ge 500000000 ]; then
  rm -rf model.$day'_part'*
else
  echo $model_cnt | mail -s 'fm_feature_model_doc_Shrink_error' 
fi

${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00000" "model."${day}"_part_0" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00001" "model."${day}"_part_1" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00002" "model."${day}"_part_2" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00003" "model."${day}"_part_3" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00004" "model."${day}"_part_4" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00005" "model."${day}"_part_5" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00006" "model."${day}"_part_6" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00007" "model."${day}"_part_7" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00008" "model."${day}"_part_8" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00009" "model."${day}"_part_9" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00010" "model."${day}"_part_10" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00011" "model."${day}"_part_11" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00012" "model."${day}"_part_12" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00013" "model."${day}"_part_13" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00014" "model."${day}"_part_14" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00015" "model."${day}"_part_15" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00016" "model."${day}"_part_16" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00017" "model."${day}"_part_17" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00018" "model."${day}"_part_18" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00019" "model."${day}"_part_19" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00020" "model."${day}"_part_20" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00021" "model."${day}"_part_21" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00022" "model."${day}"_part_22" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00023" "model."${day}"_part_23" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00024" "model."${day}"_part_24" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00025" "model."${day}"_part_25" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00026" "model."${day}"_part_26" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00027" "model."${day}"_part_27" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00028" "model."${day}"_part_28" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00029" "model."${day}"_part_29" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00030" "model."${day}"_part_30" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00031" "model."${day}"_part_31" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00032" "model."${day}"_part_32" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00033" "model."${day}"_part_33" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00034" "model."${day}"_part_34" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00035" "model."${day}"_part_35" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00036" "model."${day}"_part_36" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00037" "model."${day}"_part_37" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00038" "model."${day}"_part_38" &

wait
#rm -rf model.old*
#for files in `ls model.$day*`
#do
#    cp $files `echo ${files}|sed 's/'$day'/old/1'`
#done
cd ..


cd model_video/
${HADOOP_HOME}/bin/hadoop fs -cat /user/datacenter/recall/fm_feature/train_data/$hour/{doc,video}/*| /data/recall/tools/ftrlFM/bin/fm_train_io_parallelly -core 39 -dim 1,1,64 -w_alpha 0.27 -w_beta 1.0 -w_l1 0.01 -w_l2 0.1 -v_alpha 0.25 -v_beta 1.0 -v_l1 0.0 -v_l2 0.1 -m model.${day} -im model.${pre1_day}

model_cnt=`find ./ -name "model.${day}*"|xargs du -ck|grep total|awk 'BEGIN{sum=0} {sum+=$1}END{print sum}'`
if [ $model_cnt -ge 40000000 ]; then
  rm -rf model.${pre1_day}*
else
  echo $model_cnt | mail -s 'fm_feature_model_video_day_train_error' 
  rm -rf model.${day}*
  for files in `ls model.${pre1_day}*`
  do
    mv $files `echo ${files}|sed 's/'${pre1_day}'/'${day}'/1'`
  done
fi


model_path_sufix=$path_sufix"model_video_data/"
${HADOOP_HOME}/bin/hadoop fs -rm -r $model_path_sufix$hour
${HADOOP_HOME}/bin/hadoop fs -mkdir $model_path_sufix$hour
${HADOOP_HOME}/bin/hadoop fs -put model.${day} $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_0 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_1 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_2 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_3 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_4 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_5 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_6 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_7 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_8 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_9 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_10 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_11 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_12 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_13 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_14 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_15 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_16 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_17 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_18 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_19 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_20 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_21 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_22 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_23 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_24 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_25 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_26 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_27 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_28 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_29 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_30 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_31 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_32 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_33 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_34 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_35 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_36 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_37 $model_path_sufix$hour &
${HADOOP_HOME}/bin/hadoop fs -put model.${day}_part_38 $model_path_sufix$hour &
wait
cd ../

# 5.生成缩减后的模型
user_path_sufix="/user/datacenter/warehouse/recall_db.db/recall_pool_user_hour_detail/hour=*/*"
dim=64
cmd="$SPARK_HOME/bin/spark-submit --master yarn \
    --deploy-mode cluster \
    --num-executors 100 \
    --executor-memory 3072m \
    --executor-cores 1 \
    --driver-memory 3G \
    --name article_mf-model-Shrink_"$day" \
    --conf spark.driver.maxResultSize=3g \
    --conf spark.yarn.executor.memoryOverhead=1024 \
    --conf spark.yarn.driver.memoryOverhead=3072 \
    --conf spark.yarn.am.memoryOverhead=3072 \
    --conf spark.default.parallelism=1000 \
    --conf spark.shuffle.io.maxRetries=20 \
    --conf spark.shuffle.io.retryWait=60s \
    --conf spark.network.timeout=600s \
    --conf spark.rpc.lookupTimeout=600s \
    --conf spark.rpc.askTimeout=600s \
    --queue $queue \
    --conf spark.executor.heartbeatInterval=20s \
    --class com.netease.fm_feature_nodocid.model_Shrink \
    fm_feature_recall-1.0-SNAPSHOT.jar \
    $model_path_sufix \
    $user_path_sufix \
    $day \
    $dim"
echo '------------------------------>'
echo $cmd
$cmd

cd model_video/



model_cnt=`${HADOOP_HOME}/bin/hadoop fs -du -s ${model_path_sufix}"vaild_"${day}|cut -d ' ' -f 1 `
if [ $model_cnt -ge 40000000000 ]; then
  rm -rf model.$day'_part'*
else
  echo $model_cnt | mail -s 'fm_feature_model_video_Shrink_error' 
fi

${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00000" "model."${day}"_part_0" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00001" "model."${day}"_part_1" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00002" "model."${day}"_part_2" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00003" "model."${day}"_part_3" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00004" "model."${day}"_part_4" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00005" "model."${day}"_part_5" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00006" "model."${day}"_part_6" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00007" "model."${day}"_part_7" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00008" "model."${day}"_part_8" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00009" "model."${day}"_part_9" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00010" "model."${day}"_part_10" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00011" "model."${day}"_part_11" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00012" "model."${day}"_part_12" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00013" "model."${day}"_part_13" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00014" "model."${day}"_part_14" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00015" "model."${day}"_part_15" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00016" "model."${day}"_part_16" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00017" "model."${day}"_part_17" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00018" "model."${day}"_part_18" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00019" "model."${day}"_part_19" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00020" "model."${day}"_part_20" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00021" "model."${day}"_part_21" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00022" "model."${day}"_part_22" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00023" "model."${day}"_part_23" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00024" "model."${day}"_part_24" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00025" "model."${day}"_part_25" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00026" "model."${day}"_part_26" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00027" "model."${day}"_part_27" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00028" "model."${day}"_part_28" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00029" "model."${day}"_part_29" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00030" "model."${day}"_part_30" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00031" "model."${day}"_part_31" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00032" "model."${day}"_part_32" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00033" "model."${day}"_part_33" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00034" "model."${day}"_part_34" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00035" "model."${day}"_part_35" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00036" "model."${day}"_part_36" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00037" "model."${day}"_part_37" &
${HADOOP_HOME}/bin/hadoop fs -get ${model_path_sufix}"vaild_"${day}"/part-00038" "model."${day}"_part_38" &

wait

cd ..

del_day1=`date +%Y%m%d -d "1 day ago"`
del_day2=`date +%Y%m%d -d "2 day ago"`
del_day5=`date +%Y%m%d -d "5 day ago"`
del_day14=`date +%Y%m%d -d "4 day ago"`
del_day30=`date +%Y%m%d -d "30 day ago"`

${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/result*/*"$del_day1"*"
${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/{model_data,model_video_data}/"$del_day1"[0-2]*"
${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/{model_data,model_video_data}/vaild_"$del_day2"*"
${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/{model_data,model_video_data}/"$del_day5"*"
${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/train_data/"$del_day14"*"
${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/train_data/"$del_day1"[0-2]*"
${HADOOP_HOME}/bin/hadoop fs -rm -r "/user/datacenter/recall/fm_feature/new*/"$del_day2"*"

# 新文章试投使用的高中活用户
HQL=$HQL_Header"
add FILE mmh3.so;
add FILE decode_user_revese.py;
alter table mid_app_ctr_readtime_info add if not exists partition (daystring='$pre_1day') location '$pre_1day';
CREATE TABLE if not exists recall_pool_newdoc_user_encode (dev string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
insert OVERWRITE table recall_pool_newdoc_user_encode
select dev from mid_app_ctr_readtime_info where daystring='$pre_1day' and wherestr='Toutiao' and usertype=3 and activeall in (1,2) group by dev;

CREATE TABLE if not exists recall_pool_newdoc_user (devid string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
insert OVERWRITE table recall_pool_newdoc_user
select TRANSFORM (dev) USING 'python decode_user_revese.py' as (devid) from recall_pool_newdoc_user_encode;
"
echo $HQL
hive -e "$HQL" &

# /bin/bash run_fmFeature_recall.sh > ./logs/run_fmFeature_recall_`date +\%Y\%m\%d_\%H\%M`.log 2>&1 &
