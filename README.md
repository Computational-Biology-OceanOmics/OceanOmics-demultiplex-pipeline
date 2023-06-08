# OceanOmics Amplicon Pipeline

<p align="center">
  <img width="330" height="300" src="img/OceanOmics.png">
</p>

## Overview

This repository contains OceanOmics demultiplexing pipeline. The demultiplexing method used here is specific to OceanOmics and might not suitable for other labs.

## Usage Example

```
nextflow run /path/to/OceanOmics-demultiplex-pipeline/main.nf --raw_data "path/to/file*{R1,R2}*.fq.gz" --plate_file path/to/plates.xlsx --metadata path/to/metadata.csv --outdir output --bind_dir $(pwd) assays 16S,MiFish
```

This will set the current directory as the work directory and the output files will be saved in a subdirectory called 'output'.

You can add `-resume` to your command to resume a demultiplex run that has failed. Nextflow will attempt to resume the run at the beginning of the step that failed.

## Parameters

These can all be set on the command line or in nexflow.config. Any parameters used on the command line will be used over the nextflow.config values

- raw_data (default=null): Must have quotes and must contain '{1,2}' or '{R1,R2}' (e.g., "path/to/file*{R1,R2}*.fq.gz"),
- index_file (default=null): .csv file. Not needed if using plate file. More information about input files below,
- plate_file (default=null): .xlsx file. Not needed if using index file. More information about input files below,
- metadata (default=null): .csv file. More information about input files below,
- ulimit (default=10000): Increase this value if you encounter "cutadapt: error: [Errno 24] Too many open files",
- outdir (default=null): Directory where output files get stored. More information about output files below,
- bind_dir (default=null): Bind directory for Docker,
- assays (default=null): Can be one assay (e.g., 16S), or multiple assays separated with commas (e.g., 16S,MiFish)

## Input files

### index_file

.csv file that is not needed if using plate file.
Mandatory columns are 'sample_id', 'assay', 'index_seq_fw', 'index_seq_rv', 'full_primer_seq_fw', 'full_primer_seq_rv', 'fw_no', and 'rv_no'.

Example index_file.csv

```
sample_id,assay,index_seq_fw,index_seq_rv,full_primer_seq_fw,full_primer_seq_rv,fw_no,rv_no
sam_1,16S,TCGCCTTA,TAGATCGC,TCGCCTTAGACCCTATGGAGCTTTAGAC,TAGATCGCCGCTGTTATCCCTADRGTAACT,41F,57R
sam_2,16S,TCGCCTTA,CTCTCTAT,TCGCCTTAGACCCTATGGAGCTTTAGAC,CTCTCTATCGCTGTTATCCCTADRGTAACT,41F,58R
sam_3,16S,TCGCCTTA,TATCCTCT,TCGCCTTAGACCCTATGGAGCTTTAGAC,TATCCTCTCGCTGTTATCCCTADRGTAACT,41F,59R
```

### plate_file

.xlsx file that is not needed if using index file.

Must contain two sheet for each assay. The sheet names must be in the format of assay_plate and assay_index (e.g., 16S_plate and 16S_index).

Mandatory columns for plate sheet are 'f_primers', and all the rv primers.
Mandatory columns for index sheet are 'primer_#', 'primer_id', 'primer_seq', and 'tags'.

Example plate_file.xlsx sheet=16S_plate

```
f_primers   57R   58R   59R
41F         sam_1 sam_2 sam_3
42F         sam_4 sam_5 sam_6
43F         sam_7 sam_8 sam_9
```

Example plate_file.xlsx sheet=16S_index

```
primer_#    primer_id     primer_seq                      tags
41F         N701_16SF/D   TCGCCTTAGACCCTATGGAGCTTTAGAC    TCGCCTTA
42F         N702_16SF/D   CTAGTACGGACCCTATGGAGCTTTAGAC    CTAGTACG
43F         N703_16SF/D   TTCTGCCTGACCCTATGGAGCTTTAGAC    TTCTGCCT
57R         S501_16S2R/D  TAGATCGCCGCTGTTATCCCTADRGTAACT  TAGATCGC
58R         S502_16S2R/D  CTCTCTATCGCTGTTATCCCTADRGTAACT  CTCTCTAT
59R         S503_16S2R/D  TATCCTCTCGCTGTTATCCCTADRGTAACT  TATCCTCT
```

### metadata

.csv file that is mandatory.

The only mandatory column for the metadata file is 'sample_id'. Any additional columns will be added to sample sheet in final output. The 'sample_id' column must have the same samples found in the input index file or plate file.

## Output

The output directory will contain several subdirectories with the various outputs of the pipeline.

- A 'concat_fqs' directory will contain the fq files after trimming and concatenating (This is the final demux file directory),
- A 'cutadapt' directory will have the cutadapt output .fq files
- A 'demux_dependencies' directory will contain the files used for demultiplexing and sample file renaming,
- An 'index_file' directory will contain the index file used for createing the demux dependencies,
- A 'pipeline_info' directory will contain reports prodcude by the nextflow pipeline,
- A 'renamed_fqs' directory will contain the .fq files after renaming (there will be three directories; assigned, unknown, unnamed, plus a text file listing any samples that failed to get assigned),
- A 'seqkit_stats' directory will contain all the seqkit stats output files,
- A 'valid_input' directory will contain the input files that have been vaidated.
