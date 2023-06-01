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
if (params.raw_data) { ch_raw_data = file(params.raw_data) } else { exit 1, 'Input raw data not specified!' }
if (params.metadata) { ch_metadata = file(params.metadata) } else { exit 1, 'Input metadata not specified!' }
if (params.outdir)   { ch_outdir   = file(params.outdir)   } else { exit 1, 'Input outdir not specified!'   }
if (params.ulimit)   { ch_ulimit   = params.ulimit         } else { exit 1, 'Input ulimit not specified!'   }
if (params.assays)   { ch_assays   = params.assays         } else { exit 1, 'Input assays not specified!'   }

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
include { CREATE_SAMPLE_SHEET       } from './modules/create_sample_sheet.nf'
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
    // MODULE: Validate metadata and index or plate file file
    //
    VALIDATE_INPUT (
        ch_index,
        ch_plate,
        ch_metadata,
        ch_assays
    )

    CREATE_INDEX_FILE (
        VALIDATE_INPUT.out.valid_index,
        VALIDATE_INPUT.out.valid_plate,
        VALIDATE_INPUT.out.valid_metadata,
        ch_assays
    )

    CREATE_DEMUX_DEPENDENCIES (
        CREATE_INDEX_FILE.out.index_file,
        ch_assays
    )

    RAW_STATS (
        ch_raw_data,
        "raw"
    )

    CUTADAPT (
        ch_raw_data,
        CREATE_DEMUX_DEPENDENCIES.out.fw_index,
        CREATE_DEMUX_DEPENDENCIES.out.rv_index,
        params.ulimit
    )

    RENAME (
        CUTADAPT.out.reads,
        CREATE_DEMUX_DEPENDENCIES.out.sample_rename,
        ch_assays
    )

    DEMUX_STATS (
        RENAME.out.reads,
        "demux"
    )

    UNKNOWN_STATS (
        RENAME.out.unknown,
        "unknown"
    )

    UNNAMED_STATS (
        RENAME.out.unnamed,
        "unnamed"
    )

    TRIM_AND_CONCAT (
        RENAME.out.reads,
        CREATE_INDEX_FILE.out.index_file,
        ch_assays
    )

    CONCAT_STATS (
        TRIM_AND_CONCAT.out.reads,
        "concat"
    )

    CREATE_SAMPLE_SHEET (
        TRIM_AND_CONCAT.out.reads,
        ch_metadata
    )
}