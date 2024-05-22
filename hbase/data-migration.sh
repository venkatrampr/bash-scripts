#!/bin/bash

source="s3a://hbase-data-backup/hbase04/hbase/"
destination="hdfs://cod-cluster-master0.cdp-env.abzo-ka20.cloudera.site:8020/hbase/"
snapshot_file="/tmp/hbase-migration/snapshot_names.txt"
namespace_file="/tmp/hbase-migration/namespaces.txt"

#Get Kerberos ticket for hbase
kinit -kt /var/run/cloudera-scm-agent/process/*-hbase-*/hbase.keytab hbase/$(hostname -f)

# Generate a list of snapshots and store them in a file
hdfs dfs -ls -d $source/.hbase-snapshot/*/ | awk -F/ '!/SYSTEM/{print $NF}'  > $snapshot_file


# Export snapshots from destination to source
while IFS= read -r snapshot; do
    hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot \
        -snapshot "$snapshot" \
        -copy-from "$source" \
        -copy-to "$destination" \
        -overwrite \
        -mappers 60 \
        -bandwidth 10240 \
        -no-target-verify
done < "$snapshot_file"

#Generate a list of namespaces and store them in a file
cat $snapshot_file | awk -F/ '!/SYSTEM/ {split($NF, a, "_"); name=""; for(i=1; i<=length(a); i++) {if(tolower(a[i])==a[i]) {name=name""a[i]"_"} else {break}}; sub(/_$/, "", name); print name}' | grep -v '^$' | sort | uniq > $namespace_file

# Create namespace if any namespace already exists it continue to the next one
while IFS= read -r namespace; do
    echo "create_namespace  '$namespace'" | hbase shell
done < "$namespace_file"

# Restore snapshots
while IFS= read -r snapshot; do
    echo "restore_snapshot  '$snapshot'" | hbase shell
done < "$snapshot_file"

echo "All operations completed successfully."

