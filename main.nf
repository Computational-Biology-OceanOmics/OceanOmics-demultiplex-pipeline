#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    a4000/demultiplex_pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/a4000/demultiplex_pipeline
----------------------------------------------------------------------------------------
*/


nextflow.enable.dsl = 2

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    VALIDATE INPUTS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Validate input parameters

// Check input path parameters to see if they exist
def checkPathParamList = [ 
    params.raw_data,
    params.index_file,
    params.plate_file,
    params.metadata,
    params.outdir 
]
for (param in checkPathParamList) { if (param) { file(param, checkIfExists: true) } }

// Check mandatory parameters
if (params.raw_data) { ch_raw_data = Channel.fromFilePairs(params.raw_data) } else { exit 1, 'Input raw data not specified!' }
if (params.metadata) { ch_metadata = file(params.metadata)                  } else { exit 1, 'Input metadata not specified!' }
if (params.outdir)   { ch_outdir   = file(params.outdir)                    } else { exit 1, 'Input outdir not specified!'   }
if (params.ulimit)   { ch_ulimit   = params.ulimit                          } else { exit 1, 'Input ulimit not specified!'   }
if (params.assays)   { ch_assays   = params.assays                          } else { exit 1, 'Input assays not specified!'   }

ch_raw_data = ch_raw_data.map { it[1] }.collect() // Remove the prefix because we just need the path to the raw data

if (params.index_file )  { 
    if (params.plate_file) { 
        exit 1, 'Input index file and plate file both specified, only specify one!' 
    } else {
        ch_index = file(params.index_file)
        ch_plate = []
    }
} else {
    if (params.plate_file) { 
        ch_index = []
        ch_plate = file(params.plate_file)
    } else {
        exit 1, 'Input index file and plate file not specified, specify one!'
    }
}


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CREATE_DEMUX_DEPENDENCIES } from './modules/create_demux_dependencies.nf'
include { CREATE_INDEX_FILE         } from './modules/create_index_file.nf'
include { CUTADAPT                  } from './modules/cutadapt.nf'
include { RENAME                    } from './modules/rename.nf'
include { SEQKIT_STATS as \
          RAW_STATS;
          SEQKIT_STATS as \
          DEMUX_STATS;
          SEQKIT_STATS as \
          UNKNOWN_STATS;
          SEQKIT_STATS as \
          UNNAMED_STATS;
          SEQKIT_STATS as \
          CONCAT_STATS              } from './modules/seqkit_stats.nf'
include { TRIM_AND_CONCAT           } from './modules/trim_and_concat.nf'
include { VALIDATE_INPUT            } from './modules/validate_input.nf'

workflow DEMULTIPLEX_PIPELINE {
    //
    // MODULE: Validate metadata and index or plate file
    //
    VALIDATE_INPUT (
        ch_index,
        ch_plate,
        ch_metadata,
        ch_assays
    )

    //
    // MODULE: Create Index file if it doesn't already exist
    //
    CREATE_INDEX_FILE (
        VALIDATE_INPUT.out.valid_index,
        VALIDATE_INPUT.out.valid_plate,
        VALIDATE_INPUT.out.valid_metadata,
        ch_assays
    )

    //
    // MODULE: Create index files for Cutadapt and sample rename file 
    //
    CREATE_DEMUX_DEPENDENCIES (
        CREATE_INDEX_FILE.out.index_file,
        ch_assays
    )

    //
    // MODULE: Check stats of data before demultiplexing
    //
    RAW_STATS (
        ch_raw_data,
        "raw"
    )

    //
    // MODULE: Demultiplex
    //
    CUTADAPT (
        ch_raw_data,
        CREATE_DEMUX_DEPENDENCIES.out.fw_index,
        CREATE_DEMUX_DEPENDENCIES.out.rv_index,
        params.ulimit
    )

    //
    // MODULE: Rename samples after demultiplexing 
    //
    RENAME (
        CUTADAPT.out.reads,
        CREATE_DEMUX_DEPENDENCIES.out.sample_rename,
        ch_assays
    )

    //
    // MODULE: Check stats of reads assigned to samples after demultiplexing
    //
    DEMUX_STATS (
        RENAME.out.reads,
        "demux"
    )

    //
    // MODULE: Check stats of reads that couldn't be assigned to samples after demultiplexing
    //
    UNKNOWN_STATS (
        RENAME.out.unknown,
        "unknown"
    )

    //
    // MODULE: Check stats of reads that did get demultiplexed, but couldn't be assigned during rename step
    //
    UNNAMED_STATS (
        RENAME.out.unnamed,
        "unnamed"
    )

    //
    // MODULE: Trim leftover primers and concatenate files so that there is only one R1 and one R2 file per sample
    //
    TRIM_AND_CONCAT (
        RENAME.out.reads,
        CREATE_INDEX_FILE.out.index_file,
        ch_assays
    )

    //
    // MODULE: Check stats after trimming and concatenating files
    //
    CONCAT_STATS (
        TRIM_AND_CONCAT.out.reads,
        "concat"
    )
}

workflow {
    DEMULTIPLEX_PIPELINE ()
}