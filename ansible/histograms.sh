
nodes="18.193.48.199 18.196.243.118 35.158.109.124"

dir="${HOME}/dev/cluster-tools/ansible/logs-perf-cluster"
hist_file=${dir}/histograms.log

echo "" > ${hist_file}

for n in ${nodes}
do
    echo "" >> ${hist_file}
    echo "" >> ${hist_file}
    echo "NODE: ${n}" >> ${hist_file}
    echo "-------------------------------" >> ${hist_file}
    ~/dev/cluster-tools/ansible/histograms.py \
        ${dir}/${n}/redpanda.log >> ${hist_file}
done
