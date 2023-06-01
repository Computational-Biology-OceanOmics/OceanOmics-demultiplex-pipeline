process CREATE_DEMUX_DEPENDENCIES {
    container 'quay.io/biocontainers/r-tidyverse:1.2.1'

    input:
    path index_file
    val assays

    output:
    path "*fw.fa", emit: fw_index
    path "*rv.fa", emit: rv_index
    path "*.txt" , emit: sample_rename

    when:
    task.ext.when == null || task.ext.when

    script:
    def index_file = "$index_file"
    def assays     = "$assays"
    """
    #!/usr/bin/env Rscript

    suppressPackageStartupMessages(library(tidyverse))

    # All we need for now is the sample names and the forward and reverse
    barcodes_all <- read_csv(${index_file}) 

    #==========================================================================
    # index forward and reverse files are not correct in the original OneDrive location
    # here we reformat the files based on the ringtest_indices.csv file
    # Seb Rauschert
    #==========================================================================

    # We loop over two assays and extract the sample Fw and Rv indices and save them in separate fasta files of the format:
    # >Fw_name
    # Sequence
    # >Rv_name
    # Sequence

    assays <- str_split_1(${assays})

    for (assay in assays) {
        a <- assay
        barcodes <- barcodes_all %>%
                filter(assay %in% a)

        # Replace special characters with underscores
        barcodes\$sample_id <- gsub("[^[:alnum:]_]", "_", barcodes\$sample_id)

        # Append '_1', '_2', etc. to any duplicate samples
        duplicate_rows                    <- duplicated(barcodes\$sample_id) | duplicated(barcodes\$sample_id, fromLast = TRUE)
        underscore_numbers                <- ave(seq_along(barcodes\$sample_id), barcodes\$sample_id, FUN = function(x) seq_along(x))
        barcodes\$sample_id[duplicate_rows] <- paste0(barcodes\$sample_id[duplicate_rows], "_", underscore_numbers[duplicate_rows])

        # This will generate a .fa file that searches for both the Fw and the Rv file in R1; whilst keeping the same sample name.
        cat(paste(paste0('>', assay, '_', barcodes\$fw_no),
                  barcodes\$index_seq_fw,
                  paste0('>', assay, '_', barcodes\$rv_no),
                  barcodes\$index_seq_rv, sep='\n'), 
                  sep = '\n', 
                  file = paste0(assay, '_fw.fa'))

        cat(paste(paste0('>',assay,'_',barcodes\$rv_no),
                  barcodes\$index_seq_rv,
                  paste0('>',assay,'_',barcodes\$fw_no),
                  barcodes\$index_seq_fw, sep='\n'), 
                  sep = '\n', 
                  file = paste0(assay, '_rv.fa'))

        # This section creates a file to rename the demultiplexed files to reflect the sample name, including the assay
        cat(paste0(assay,'_', barcodes\$fw_no, '-', assay, "_", barcodes\$rv_no, '.R[12].fq.gz ', barcodes\$sample_id ,'_',assay,'_forward.#1.fq.gz'),
            paste0(assay,'_', barcodes\$rv_no, '-', assay, "_", barcodes\$fw_no, '.R[12].fq.gz ', barcodes\$sample_id ,'_',assay,'_reverse.#1.fq.gz'),
            sep='\n',
            file=paste0(assay, '_sample_name_rename_pattern.txt'))
        }
    
    """
}