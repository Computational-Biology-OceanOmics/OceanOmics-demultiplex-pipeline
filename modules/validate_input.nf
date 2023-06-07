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
    path "*plate_file.xlsx"   , emit: valid_plate

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
    import warnings
    warnings.filterwarnings("ignore", category = UserWarning, module = "openpyxl")
    assay_list = ${assays}.split(",")

    # Validate and import metadata file
    try:
        metadata_df = pd.read_csv(${metadata})
        if "sample_id" not in metadata_df.columns:
            raise AssertionError("metadata file: " + ${metadata} + " - missing 'sample_id' column")
        if metadata_df["sample_id"].isnull().values.any():
            raise AssertionError("metadata file: " + ${metadata} + " - 'sample_id' column can't contain any NA values")

        # Save valid file
        metadata_df.to_csv("valid_metadata.csv", index=False)

    except FileNotFoundError:
        print("metadata file: " + ${metadata} + " - not found")
        sys.exit(1)
    except PermissionError:
        print("metadata file: " + ${metadata} + " - permission denied")
        sys.exit(1)
    except pd.errors.ParserError as e:
        print("metadata file: " + ${metadata} + " - error parsing csv file\\n" + str(e))
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
            if index_df["sample_id"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'sample_id' column can't contain any NA values")
            if "assay" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'assay' column")
            if index_df["assay"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'assay' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["assay"].isna()]["sample_id"]))))
            if "index_seq_fw" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'index_seq_fw' column")
            if index_df["index_seq_fw"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'index_seq_fw' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["index_seq_fw"].isna()]["sample_id"]))))
            if "index_seq_rv" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'index_seq_rv' column")
            if index_df["index_seq_rv"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'index_seq_rv' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["index_seq_rv"].isna()]["sample_id"]))))
            if "full_primer_seq_fw" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'full_primer_seq_fw' column")
            if index_df["full_primer_seq_fw"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'full_primer_seq_fw' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["full_primer_seq_fw"].isna()]["sample_id"]))))
            if "full_primer_seq_rv" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'full_primer_seq_rv' column")
            if index_df["full_primer_seq_rv"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'full_primer_seq_rv' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["full_primer_seq_rv"].isna()]["sample_id"]))))
            if "fw_no" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'fw_no' column")
            if index_df["fw_no"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'fw_no' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["fw_no"].isna()]["sample_id"]))))
            if "rv_no" not in index_df.columns:
                raise AssertionError("index file: " + ${index_file} + " - missing 'rv_no' column")
            if index_df["rv_no"].isnull().values.any():
                raise AssertionError("index file: " + ${index_file} + " - 'rv_no' column can't contain any NA values. Samples with NAs: " + 
                str(sorted(list(index_df[index_df["rv_no"].isna()]["sample_id"]))))

            for assay in assay_list:
                # Make sure the assay parameter is found in the index file
                if assay not in list(index_df["assay"]):
                    raise AssertionError("assay: " + assay + " - not found in " + ${index_file})

                # An index file can contain multiple assays, we just want the rows for the current assay
                curr_assay_df = index_df[index_df["assay"] == assay]

                # Make sure there are no duplicate samples
                if len(curr_assay_df["sample_id"]) != len(set(curr_assay_df["sample_id"])):
                    raise AssertionError("index file: " + ${index_file} + " - sample_id column can't contain duplicate samples within an assay. Duplicate samples: " +
                    str(sorted(list(curr_assay_df[curr_assay_df.duplicated(subset="sample_id", keep=False)]["sample_id"].unique()))))

                # Make sure the samples in the index file matches the metadata
                if sorted(set(curr_assay_df["sample_id"])) != sorted(set(metadata_df["sample_id"])):
                    raise AssertionError("index file: " + ${index_file} + " - sample_id column must have the same samples as metadata file. Samples missing from index file: " +
                    str(sorted(set(metadata_df["sample_id"]) - set(curr_assay_df["sample_id"]))) + ", samples missing from metadata: " +
                    str(sorted(set(curr_assay_df["sample_id"]) - set(metadata_df["sample_id"]))))

            # Save valid file
            index_df.to_csv("valid_index_file.csv", index=False)

        except FileNotFoundError:
            print("index file: " + ${index_file} + " - not found")
            sys.exit(1)
        except PermissionError:
            print("index file: " + ${index_file} + " - permission denied")
            sys.exit(1)
        except pd.errors.ParserError as e:
            print("index file: " + ${index_file} + " - error parsing csv file\\n" + str(e))
            sys.exit(1)
        except AssertionError as e:
            print(e)
            sys.exit(1)
    
    # Create empty file if index option wasn't used to avoid error with Nextflow not finding output files
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
        
                # The first column should be 'f_primers' and it can't contain NA values
                if curr_plate_df.columns[0] != "f_primers":
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet must start with f_primers column")
                if curr_plate_df["f_primers"].isnull().values.any():
                    raise AssertionError("plate file: " + ${plate_file} + " - 'f_primers' column can't contain any NA values")

                # Make sure there are no duplicate 'f_primers'
                if len(set(curr_plate_df["f_primers"])) != len(curr_plate_df["f_primers"]):
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet can't contain duplicate f_primers. Duplicate f_primers: " +
                    str(sorted(list(curr_plate_df[curr_plate_df.duplicated(subset="f_primers", keep=False)]["f_primers"].unique()))))

                # Pandas will add a '.' and a number to a column name if there are duplicate columns. We can use this to check for duplicates.
                if len([col for col in curr_plate_df.columns if '.' in col]) > 0:
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet can't contain duplicate columns, or columns containing '.'. Offending columns: " +
                    str(sorted(list([col for col in curr_plate_df.columns if '.' in col]))))
                
                # Get a list of all samples and primers in plate sheet
                curr_samples_df = curr_plate_df.iloc[:, 1:]
                sample_list = []
                primer_list = list(curr_plate_df["f_primers"])
                for col in curr_samples_df.columns:
                    primer_list.append(col)
                    for row in range(len(curr_samples_df.index)):
                        if not pd.isna(curr_samples_df.at[row,col]):
                            sample_list.append(str(curr_samples_df.at[row,col]))
                
                # Make sure there are no duplicate samples in plate sheet
                if len(set(sample_list)) != len(sample_list):
                    duplicates = []
                    for i in sample_list:
                        if sample_list.count(i) > 1:
                            duplicates.append(i)
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet can't have duplicate samples. Duplicate samples: " +
                    str(sorted(duplicates)))

                # make sure samples in plate sheet match metadata
                if sorted(set(sample_list)) != sorted(set(metadata_df["sample_id"])):
                    raise AssertionError("plate file: " + ${plate_file} + " - plate sheet samples must be the same as the samples in metadata file. Samples missing from plate sheet: " +
                    str(sorted(set(metadata_df["sample_id"]) - set(sample_list))) + ", samples missing from metadata: " +
                    str(sorted(set(sample_list) - set(metadata_df["sample_id"]))))

                # Save valid file
                curr_plate_df.to_csv("valid_plate_" + assay +".csv", index=False)

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
            
            # Import the index sheet
            try:
                curr_index_df = pd.read_excel(${plate_file}, sheet_name = assay + "_index")
                df_dict[assay + "_index"] = curr_index_df
    
                # Validate columns
                if "primer_#" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'primer_#' column")
                if curr_index_df["primer_#"].isnull().values.any():
                    raise AssertionError("plate file: " + ${plate_file} + " - 'primer_#' column can't contain any NA values")
                if "primer_id" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'primer_id' column")
                if curr_index_df["primer_id"].isnull().values.any():
                    raise AssertionError("plate file: " + ${plate_file} + " - 'primer_id' column can't contain any NA values. Primers with NAs: " + 
                    str(sorted(list(curr_index_df[curr_index_df["primer_id"].isna()]["primer_#"]))))
                if "primer_seq" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'primer_seq' column")
                if curr_index_df["primer_seq"].isnull().values.any():
                    raise AssertionError("plate file: " + ${plate_file} + " - 'primer_seq' column can't contain any NA values. Primers with NAs: " + 
                    str(sorted(list(curr_index_df[curr_index_df["primer_seq"].isna()]["primer_#"]))))
                if "tags" not in curr_index_df.columns:
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet must have 'tags' column")
                if curr_index_df["tags"].isnull().values.any():
                    raise AssertionError("plate file: " + ${plate_file} + " - 'tags' column can't contain any NA values. Primers with NAs: " + 
                    str(sorted(list(curr_index_df[curr_index_df["tags"].isna()]["primer_#"]))))
                
                # Make sure we don't have duplicate primers
                if len(set(curr_index_df["primer_#"])) != len(curr_index_df["primer_#"]):
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet can't contain duplicate values in 'primer_#' column" +
                    str(sorted(list(curr_index_df[curr_index_df.duplicated(subset="primer_#", keep=False)]["primer_#"].unique()))))
                if len(set(curr_index_df["primer_seq"])) != len(curr_index_df["primer_seq"]):
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet can't contain duplicate values in 'primer_seq' column" +
                    str(sorted(list(curr_index_df[curr_index_df.duplicated(subset="primer_seq", keep=False)]["primer_seq"].unique()))))
                if len(set(curr_index_df["tags"])) != len(curr_index_df["tags"]):
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet can't contain duplicate values in 'tags' column" +
                    str(sorted(list(curr_index_df[curr_index_df.duplicated(subset="tags", keep=False)]["tags"].unique()))))

                # Make sure primers in index sheet and plate sheet match    
                if sorted(set(curr_index_df["primer_#"])) != sorted(set(primer_list)):
                    raise AssertionError("plate file: " + ${plate_file} + " - index sheet and plate sheet must contain same primers. Primers missing from plate sheet: " +
                    str(sorted(set(curr_index_df["primer_#"]) - set(primer_list))) + ", primers missing from index sheet: " +
                    str(sorted(set(primer_list) - set(curr_index_df["primer_#"]))))

                # Save valid file
                curr_index_df.to_csv("valid_index_" + assay +".csv", index=False)

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
        
        # Use the dictionary we created earlier to create the Excel file
        writer = pd.ExcelWriter("valid_plate_file.xlsx")
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        writer.close()
    
    # Create empty file if plate option wasn't used to avoid error with Nextflow not finding output files
    else:
        with open("empty_plate_file.xlsx", 'w') as file:
            pass
    """
}