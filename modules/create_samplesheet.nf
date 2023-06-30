process CREATE_SAMPLESHEET {
    container 'adbennett/pandas_excel:2.0.2'

    input:
    path metadata
    val assays 
    val outdir

    output:
    path "*.csv", emit: samplesheets

    when:
    task.ext.when == null || task.ext.when

    script:
    def metadata   = "'$metadata'"
    def assays     = "'$assays'"
    def outdir     = "'$outdir'"
    """
    #!/usr/bin/env python3

    # Setup
    import pandas as pd

    assay_list = ${assays}.split(",")
    print(assay_list)
    for assay in assay_list:
        metadata_df = pd.read_csv(${metadata}, dtype=str)
        metadata_df = metadata_df.rename(columns={"sample_id": "sample"})
        metadata_df["control"] = metadata_df["sample"].str.contains("WC|FC|EB")
        metadata_df["fastq_1"] = ${outdir} + "/concat_fqs/" + metadata_df["sample"] + "_" + assay + ".1.fq.gz"
        metadata_df["fastq_2"] = ${outdir} + "/concat_fqs/" + metadata_df["sample"] + "_" + assay + ".2.fq.gz"

        # Save samplesheet
        metadata_df.to_csv(str(assay) + "_samplesheet.csv", index=False)
    """
}