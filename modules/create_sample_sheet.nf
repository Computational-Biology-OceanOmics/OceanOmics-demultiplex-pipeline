process CREATE_SAMPLE_SHEET {
    container 'ubuntu:20.04'

    input:
    path reads, 
    path metadata

    output:
    path "*.csv", emit: sample_sheet

    when:
    task.ext.when == null || task.ext.when

    script:
    """

    """
}