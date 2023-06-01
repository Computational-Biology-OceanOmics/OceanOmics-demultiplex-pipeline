process TRIM_AND_CONCAT {
    container 'ubuntu:20.04'

    input:
    path table 
    path index_file
    val assays

    output:
    path "*.fq.gz", emit: reads

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    # Function to trim and concatenate files
    process_files() {
        file1=\$1
        file2=\$2
        suffix=\$3
        forward_trim=\$4
        reverse_trim=\$5
        prefix=$(basename "\$file2" | rev | cut -d '_' -f 2- | rev)

        if [[ "\$suffix" == "1.fq" ]]; then
            # Trim reads
            zcat "\$file1" | sed "0~2s/^.\\{\$forward_trim\\}//g" > "\${prefix}_trimmed_forward.\${suffix}"
            zcat "\$file2" | sed "0~2s/^.\\{\$reverse_trim\\}//g" > "\${prefix}_trimmed_reverse.\${suffix}"
        else
            # Trim reads
            zcat "\$file1" | sed "0~2s/^.\\{\$reverse_trim\}//g" > "\${prefix}_trimmed_forward.\${suffix}"
            zcat "\$file2" | sed "0~2s/^.\\{\$forward_trim\\}//g" > "\${prefix}_trimmed_reverse.\${suffix}"
        fi

        # Concatenate files
        cat "\${prefix}_trimmed_forward.\${suffix}" "\${prefix}_trimmed_reverse.\${suffix}" | gzip > "\${prefix}_concat.\${suffix}.gz"

        # Remove files that aren't needed any more
        rm "\$file1" "\$file2" "\${prefix}_trimmed_forward.\${suffix}" "\${prefix}_trimmed_reverse.\${suffix}"
    }
    export -f process_files

    IFS=',' read -ra assays <<< "$assays"
    for a in "\${assays[@]}"; do
        if [[ "\${a^^}" == "16S" ]]; then
            forward_trim=20
            reverse_trim=22
        elif [[ "\${a^^}" == "MIFISH" ]]; then
            forward_trim=21
            reverse_trim=27
        elif [[ "\${a^^}" == "COI" ]]; then
            forward_trim=26
            reverse_trim=26
        else
            forward_trim=0
            reverse_trim=0
        fi

        filesFw=(*\${a}*1.fq.gz)
        filesRv=(*\${a}*2.fq.gz)

        # Loop over read 1 files (there should be two read 1 files for each sample)
        for ((i = 0; i < \${#filesFw[@]}; i += 2)); do
            file1=\${filesFw[i]}
            file2=\${filesFw[i+1]}

            # Trim and concatenate the files
            process_files "\$file1" "\$file2" "1.fq" \$forward_trim \$reverse_trim &
        done
        wait

        # Loop over read 2 files (there should be two read 2 files for each sample)
        for ((i = 0; i < \${#filesRv[@]}; i += 2)); do
            file1=\${filesRv[i]}
            file2=\${filesRv[i+1]}

            # Trim and concatenate the files
            process_files "\$file1" "\$file2" "2.fq" \$forward_trim \$reverse_trim &
        done
        wait
    done

    for file in *_concat.*; do 
        mv -- "\$file" "\${file//_concat/}"; 
    done
    """
}