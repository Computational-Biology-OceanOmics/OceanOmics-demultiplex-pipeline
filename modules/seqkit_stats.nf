process SEQKIT_STATS {
    container 'biocontainers/seqkit:2.4.0--h9ee0642_0'

    input:
    path reads
    val prefix 

    output:
    path "*.txt", emit: stats

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    # Create stats and save to file
    seqkit stats -j ${task.cpus} -b ${reads} -a > ${prefix}_seqkit_stats.txt
    """
}