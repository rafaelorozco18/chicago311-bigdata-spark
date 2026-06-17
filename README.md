# chicago311-bigdata-spark
Processed 13.78M records (4.9 GB) on a PySpark/YARN/HDFS cluster. Built an end-to-end pipeline with BroadcastHashJoin, Window functions, and lazy chained transformations. Random Forest (MLlib) achieved AUC-ROC 0.81 predicting late-resolution tickets; SR_TYPE and ward-level patterns were the top drivers.
