import pandas as pd
import argparse
import os

def extract_and_print_statistics(file_path: str) -> None:
    try:
        # Determine file type and load accordingly
        _, ext = os.path.splitext(file_path)
        if ext.lower() == '.xlsx':
            df = pd.read_excel(file_path, engine='openpyxl')
        elif ext.lower() == '.csv':
            df = pd.read_csv(file_path)
        else:
            raise ValueError("Unsupported file type. Use .xlsx or .csv.")
        
        uni = os.path.basename(file_path).split('_')[0]  # Infer university name from filename (e.g., 'texas_a_and_m_university')
        
        # Force key columns to string type to avoid type inference issues (e.g., int instead of str)
        for col in ['tier', 'tier1_labels', 'hydrogen_specific', 'tier1_unsure', 'tier2_topic']:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
            else:
                print(f"⚠️ Warning: Column '{col}' not found in the file. Skipping related stats.")
        
        total_courses = len(df)
        
        # Tier calculations
        df['tier_list'] = df['tier'].apply(lambda x: [int(t.strip()) for t in x.split(',') if t.strip().isdigit()])
        error_courses = len(df[df['tier'] == 'ERROR'])
        tier0_courses = len(df[df['tier_list'].apply(lambda x: 0 in x and 1 not in x and 2 not in x)])
        tier1_courses = len(df[df['tier_list'].apply(lambda x: 1 in x)])
        # Stricter Tier 2: Require 2 in tier OR non-empty/non-nan hydrogen_specific
        tier2_courses = len(df[
            df['tier_list'].apply(lambda x: 2 in x) | 
            ((df['hydrogen_specific'].str.strip() != '') & (df['hydrogen_specific'] != 'nan'))
        ])
        
        # Tier 1 subclasses (dynamic count of unique labels, accounting for multiples)
        tier1_df = df[df['tier_list'].apply(lambda x: 1 in x)].copy()
        tier1_df['tier1_labels_list'] = tier1_df['tier1_labels'].apply(lambda x: [l.strip() for l in x.split(',') if l.strip()])
        tier1_subclass_counts = pd.Series([label for labels in tier1_df['tier1_labels_list'] for label in labels]).value_counts().to_dict()
        tier1_unsure = len(tier1_df[tier1_df['tier1_unsure'].str.strip() != ''])
        
        # Tier 2 subclasses (dynamic count of unique hydrogen_specific values, accounting for multiples; exclude 'nan')
        tier2_df = df[
            df['tier_list'].apply(lambda x: 2 in x) | 
            ((df['hydrogen_specific'].str.strip() != '') & (df['hydrogen_specific'] != 'nan'))
        ].copy()
        tier2_df['hydrogen_specific_list'] = tier2_df['hydrogen_specific'].apply(lambda x: [h.strip() for h in x.split(',') if h.strip() and h != 'nan'])
        tier2_subclass_counts = pd.Series([hs for hs_list in tier2_df['hydrogen_specific_list'] for hs in hs_list if hs != 'nan']).value_counts().to_dict()
        tier2_multi_subclass = len(tier2_df[tier2_df['hydrogen_specific_list'].apply(len) > 1])  # New: Count courses with >1 subclass
        
        # Tier 2 topic stats (value_counts of unique topics; limit to top 10 for brevity)
        tier2_topic_counts = tier2_df['tier2_topic'].value_counts().head(10).to_dict()
        
        # Print statistics
        print(f"\n📈 Statistics for {uni} (from {file_path}):")
        print(f"  - Total Courses: {total_courses}")
        print(f"  - Courses with Errors: {error_courses}")
        print(f"  - Tier 0 Courses: {tier0_courses}")
        print(f"  - Tier 1 Courses: {tier1_courses}")
        if tier1_subclass_counts:
            print(f"    - Subclass Counts (accounting for multiples): {', '.join([f'{k}: {v}' for k, v in tier1_subclass_counts.items()])}")
        else:
            print("    - Subclass Counts: None")
        print(f"    - Unsure in Tier 1: {tier1_unsure}")
        print(f"  - Tier 2 Courses: {tier2_courses}")
        if tier2_subclass_counts:
            print(f"    - Subclass Counts (accounting for multiples): {', '.join([f'{k}: {v}' for k, v in tier2_subclass_counts.items()])}")
            print(f"    - Courses with Multiple Subclasses: {tier2_multi_subclass}")
        else:
            print("    - Subclass Counts: None")
        if tier2_topic_counts:
            print(f"    - Top Topic Counts: {', '.join([f'{k}: {v}' for k, v in tier2_topic_counts.items()])}")
            print("      (Note: These are counts of unique 'tier2_topic' values, which may represent descriptions rather than subclasses.)")
        else:
            print("    - Top Topic Counts: None")
        
    except ModuleNotFoundError as e:
        if 'openpyxl' in str(e):
            print(f"❌ Error: Missing 'openpyxl' module for reading Excel files. Install it via `pip install openpyxl`.")
        else:
            print(f"❌ Error loading file: {e}")
    except Exception as e:
        print(f"❌ Could not extract statistics from {file_path}. Error: {e}")
        if "'int' object has no attribute 'split'" in str(e):
            print("   Hint: This may be due to numeric type inference in the 'tier' column. Ensure all values are strings in the file.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract statistics from a tiered taxonomy Excel or CSV file.")
    parser.add_argument("file_path", type=str, help="Path to the .xlsx or .csv file (e.g., /path/to/texas_a_and_m_university_tiered_taxonomy_twopass.xlsx)")
    args = parser.parse_args()
    
    extract_and_print_statistics(args.file_path)