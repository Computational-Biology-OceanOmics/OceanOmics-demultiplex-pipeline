process RENAME {
    container 'ubuntu:20.04'

    input:
    path reads
    path sample_rename
    val assays

    output:
    path "assigned/*.fq.gz"   , emit: reads
    path "unknown/*.fq.gz"    , emit: unknown
    path "unnamed/*.fq.gz"    , emit: unnamed
    path "missing_samples.txt", emit: missing

    when:
    task.ext.when == null || task.ext.when

    script:
    """ 
    cat ${sample_rename} > sample_rename_concat.txt
    
    # Here we reference the rename pattern files in the raw data folder, which contains the information that maps the 
    # index ID pairings with the sample IDs
    mmv < sample_rename_concat.txt -g
   
    # move the unnamed and unknowns into separate folders 
    mkdir -p assigned unknown unnamed
    mv *unknown*.fq.gz unknown
    
    IFS=',' read -ra assays <<< "$assays"
    for a in "\${assays[@]}"; do
        mv \${a}_*.fq.gz unnamed
    done

    # Check if any samples in rename file didn't get demultiplexed
    samples=$(awk '{split(\$2, a, ".#1.fq.gz"); print a[1]}' "sample_rename_concat.txt")
    missing_samples=()
    for sample in \$samples; do
        if ! compgen -G "\$sample*"; then
            missing_samples+=("\$sample")
        fi
    done

    mv *.fq.gz assigned

    # Print the missing sample
    printf "%s\n" "Missing samples: \${missing_samples[@]}" > missing_samples.txt

    """
}