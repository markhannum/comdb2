#!/bin/bash

export results=./results.$$.txt

export TXNSIZE=2
export ITERS=1000
export THDS=5

function log_entry
{
    typeset cols=$1
    typeset txnsz=$2
    typeset thds=$3
    typeset iters=$4
    typeset ro_deadlock=$5
    typeset ro_time=$6
    typeset non_ro_deadlock=$7
    typeset non_ro_time=$8
    typeset ts=$(date +%Y-%m-%d-%H:%M:%S)

    echo "$ts cols:$cols txnsz:$txnsz thds:$thds iters:$iters reorder-deadlock:$ro_deadlock reorder-time:$ro_time non-reorder-deadlock:$non_ro_deadlock non-reorder-time:$non_ro_time" >> $results
}

for y in 2 4 8 16 32 64 128 256 512 1024; do

    export TXNSIZE=$y

    for x in 1 2 4 8 16 32 ; do

        export NUMCOLUMNS=$x
        echo "RUNNING NUMCOLUMNS $NUMCOLUMNS TXNSIZE $TXNSIZE"

        cp reorder.test/lrl.reordered.options reorder.test/lrl.options
        make reorder
        if [[ $? != 0 ]]; then
            echo "Failed testcase"
            break 2
        fi

        dir=$(ls -lt | egrep "^d" | egrep test_ | head -1 | awk '{print $NF}')
        testcase=$(ls $dir/logs/*testcase)
        reordered_deadlocks=$(egrep "Number of deadlocks" $testcase | awk '{print $7}')
        reordered_time=$(egrep "Duration" $testcase | awk '{print $2}')

        cp reorder.test/lrl.normal.options reorder.test/lrl.options
        make reorder
        if [[ $? != 0 ]]; then
            echo "Failed testcase"
            break 2
        fi

        dir=$(ls -lt | egrep "^d" | egrep test_ | head -1 | awk '{print $NF}')
        testcase=$(ls $dir/logs/*testcase)
        non_reordered_deadlocks=$(egrep "Number of deadlocks" $testcase | awk '{print $7}')
        non_reordered_time=$(egrep "Duration" $testcase | awk '{print $2}')
        cp reorder.test/lrl.reordered.options reorder.test/lrl.options

        log_entry $NUMCOLUMNS $TXNSIZE $THDS $ITERS $reordered_deadlocks $reordered_time $non_reordered_deadlocks $non_reordered_time

    done
done



