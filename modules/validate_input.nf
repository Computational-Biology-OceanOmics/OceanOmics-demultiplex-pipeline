process VALIDATE_INPUT {
    container 'adbennett/pandas_excel:2.0.2'

    input:
    path index_file
    path plate_file
    path metadata
    val assays

    output:
    path "valid_metadata.csv", emit: valid_metadata
    path "*index_file.csv"   , emit: valid_index
    path "*plate_file.csv"   , emit: valid_plate

    when:
    task.ext.when == null || task.ext.when

    script:
    def index_file = "'$index_file'"
    def plate_file = "'$plate_file'"
    def metadata   = "'$metadata'"
    def assays     = "'$assays'"
    """
    #!/usr/bin/env python3

    # Setup
    import pandas as pd
    import sys
    assay_list = ${assays}.split(",")

    # Validate and import metadata file
    try:
        metadata_df = pd.read_csv(${metadata})
        if "sample_id" not in metadata_df.columns:
            raise AssertionError("metadata file: " + ${metadata} + " - missing 'sample_id' column")

        # Save valid file
        metadata_df.to_csv("valid_metadata.csv", index=False)

    except FileNotFoundError:
        print("metadata file: " + ${metadata} + " - not found")
        sys.exit(1)
    except PermissionError:
        print("metadata file: " + ${metadata} + " - permission denied")
        sys.exit(1)
    except AssertionError as e:
        print(e)
        sys.exit(1)


    # Make sure index or plate file option is used, not both
    try:
        if ${index_file} == "" and ${plate_file} == "":
            raise AssertionError("index file and plate file options are both missing, Please provide 1")
        if ${index_file} != "" and ${plate_file} != "":
            raise AssertionError("index file and plate file options are both provided, Please provide only 1")

    except AssertionError as e:
        print(e)
        sys.exit(1)
    

    # Validate and import index file
    if ${index_file} != "":
        try:
            index_df = pd.read_csv(${index_file})
    
            # Validate columns
            if "sample_id" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'sample_id' column")
            if "assay" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'assay' column")
            if "index_seq_fw" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'index_seq_fw' column")
            if "index_seq_rv" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'index_seq_rv' column")
            if "full_primer_seq_fw" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'full_primer_seq_fw' column")
            if "full_primer_seq_rv" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'full_primer_seq_rv' column")
            if "fw_no" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'fw_no' column")
            if "rv_no" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'rv_no' column")

            # Validate individual columns
            if sorted(set(index_df["assay"])) != sorted(set(assay_list)):
                raise AssertionError("index file: " + ${index_file} + " - assay column must contain the same assays as --assays parameter")

            for assay in assay_list:
                curr_assay_df = index_df[index_df["assay"] == assay]
                if len(curr_assay_df["sample_id"]) != len(set(curr_assay_df["sample_id"])):
                    raise AssertionError("index file: " + ${index_file} + " - sample_id column can't contain duplicate samples within an assay")
                if sorted(set(curr_assay_df["sample_id"])) != sorted(set(metadata_df["sample_id"])):
                    raise AssertionError("index file: " + ${index_file} + " - sample_id column must have the same samples as metadata file")
            
            # Save valid file
            index_df.to_csv("valid_index_file.csv", index=False)

        except FileNotFoundError:
            print("index file: " + ${index_file} + " - not found")
            sys.exit(1)
        except PermissionError:
            print("index file: " + ${index_file} + " - permission denied")
            sys.exit(1)
        except AssertionError as e:
            print(e)
            sys.exit(1)
    else:
        with open("empty_index_file.csv", 'w') as file:
            pass

    # Validate and import plate file (There should be a plate sheet and an index sheet for each assay)
    if ${plate_file} != "":

        # Store each data frame in a dictionary to create an Excel file with a dynamic number of sheets
        df_dict = dict()
        for assay in assay_list:
            # First import the plate sheet
            try:
                curr_plate_df = pd.read_excel(${plate_file}, sheet_name = assay + "_plate")
                df_dict[assay + "_plate"] = curr_plate_df
        
                if curr_plate_df.columns[0] != "f_primers":
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet must start with f_primers column")
                if len([col for col in curr_plate_df.columns if '.' in col]) > 0:
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet can't contain duplicate columns, or columns containing '.'")
                if len(set(curr_plate_df["f_primers"])) != len(curr_plate_df["f_primers"]):
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet can't contain duplicate f_primers")
                
                # Get a list of all samples in plate sheet for sample validation
                curr_samples_df = curr_plate_df.iloc[:, 1:]
                sample_list = []
                for col in curr_samples_df.columns:
                    for row in range(len(curr_samples_df.index)):
                        if not pd.isna(curr_samples_df.at[row,col]):
                            sample_list.append(curr_samples_df.at[row,col])
                
                if len(set(sample_list)) != len(sample_list):
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet can't have duplicate samples")
                if sorted(set(sample_list)) != sorted(set(metadata_df["sample_id"])):
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet samples must be the same as the samples in metadata file. Samples missing from plate sheet: " +
                    str(set(metadata_df["sample_id"]) - set(sample_list)) + ", samples missing from metadata: " +
                    str(set(sample_list) - set(metadata_df["sample_id"])))

                # Save valid file
                curr_plate_df.to_csv("valid_plate" + assay +".csv", index=False)

            except FileNotFoundError:
                print("plate file: " + ${plate_file} + " - not found")
                sys.exit(1)
            except PermissionError:
                print("plate file: " + ${plate_file} + " - permission denied")
                sys.exit(1)
            except ValueError:
                print("plate file: " + ${plate_file} + " - '" + assay + "_plate' sheet not found")
                sys.exit(1)
            except AssertionError as e:
                print(e)
                sys.exit(1)
            

            # Then import the index sheet
            try:
                curr_index_df = pd.read_excel(${plate_file}, sheet_name = assay + "_index")
                df_dict[assay + "_index"] = curr_index_df
    
                if "primer_#" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'primer_#' column")
                if "primer_id" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'primer_id' column")
                if "primer_seq" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'primer_seq' column")
                if "tags" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'tags' column")
                
                if len(set(curr_index_df["primer_#"])) != len(curr_index_df["primer_#"]):
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet can't contain duplicate values in 'primer_#' column")
                if sorted(set(curr_index_df["primer_#"])) != sorted(set(sample_list)):
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet and plate sheet must contain same primers (e.g., if one sheet has 1F, the other sheet should also have 1F)")

                # Save valid file
                curr_index_df.to_csv("valid_index" + assay +".csv", index=False)

            except FileNotFoundError:
                print("plate file: " + ${plate_file} + " - not found")
                sys.exit(1)
            except PermissionError:
                print("plate file: " + ${plate_file} + " - permission denied")
                sys.exit(1)
            except ValueError:
                print("plate file: " + ${plate_file} + " - '" + assay + "_index' sheet not found")
                sys.exit(1)
            except AssertionError as e:
                print(e)
                sys.exit(1)
        
        writer = pd.ExcelWriter("valid_plate_file.xlsx")
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Save the Excel file
        writer.save()
    
    else:
        with open("empty_plate_file.xlsx", 'w') as file:
            pass
    """
}