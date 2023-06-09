process CUTADAPT {
    container 'quay.io/biocontainers/cutadapt:4.1--py37h8902056_1'

    input:
    path raw_data
    path fw_index 
    path rv_index
    val ulimit
    val assays

    output:
    path "*.fq.gz", emit: reads

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    #!/bin/bash

    # Too avoid too many open files error:
    ulimit -S -n ${ulimit}

    IFS=',' read -ra assay_array <<< "$assays"

    for a in "\${assay_array[@]}"
    do

        #..........................................................................................
        # | The -g and -G option specify that we are dealing with combinatorial adapters.
        # | As per cutadapt documentation llumina’s combinatorial dual indexing strategy uses
        # | a set of indexed adapters on R1 and another one on R2. Unlike unique dual indexes (UDI),
        # | all combinations of indexes are possible.
        # | this is another difference: the output will assign the name from the forward and reverse
        # | reads that were identified with the dual index
        # |
        # |the '^' in front of file (^file:) means that we anchor the tags to the beginning of the read!
        #..........................................................................................

        cutadapt -j ${task.cpus} \
                 -e 0.15 \
                 --no-indels \
                 -g ^file:\${a}_fw.fa  \
                 -G ^file:\${a}_rv.fa \
                 -o {name1}-{name2}.R1.fq.gz \
                 -p {name1}-{name2}.R2.fq.gz \
                 --report=full \
                 --minimum-length 1 \
                 ${raw_data}
    done
    """
}