import pandas as pd
import dask
import dask.dataframe as dd
import itertools
import pyarrow as pa
import pyarrow.parquet as pq


from dask.distributed import Client
from dask_jobqueue import PBSCluster

cluster = PBSCluster(
                     memory="500GB",
                     project='P48500028',
                     queue='premium',
                     interface='ib0')

cluster.scale(1000)  # Start 100 workers in 100 jobs that match the description above



from dask.distributed import Client
client = Client(cluster) 

# Example calculation function
def calculate_values(cA, cB, E1, E2, reference_df):
    Num1A = reference_df.loc[cA, E1]
    Num2A = reference_df.loc[cA, E2]
    Num1B = reference_df.loc[cB, E1]
    Num2B = reference_df.loc[cB, E2]

    # Return the row as a dictionary
    # Build the row dictionary
    return {
        "ConditionA": cA,
        "ConditionB": cB,
        "geneA": E1,
        "geneB": E2,
        "mediangAcA": Num1A,
        "mediangBcA": Num2A,
        "mediangAcB": Num1B,
        "mediangBcB": Num2B,
    }

# Function to process pairs and write directly to Parquet using Dask
def process_and_write_parquet(condition_pairs, element_pairs, reference_df, chunk_size=10000):
    rows = []
    chunk_count = 0

    for condition_pair in condition_pairs:
        for element_pair in element_pairs:
            cA, cB = condition_pair
            E1, E2 = element_pair
            
            # Calculate the values and construct the row
            row = calculate_values(cA, cB, E1, E2, reference_df)
            rows.append(row)

            # Write to Parquet when the chunk size is reached
            if len(rows) >= chunk_size:
                write_chunk_to_parquet(rows, chunk_count)
                chunk_count += 1
                rows = []

    # Write any remaining rows after the final chunk
    if rows:
        write_chunk_to_parquet(rows, chunk_count)

# Function to write a chunk of rows to a Parquet file
def write_chunk_to_parquet(rows, chunk_count):
    # Convert the list of dictionaries to a DataFrame
    df_chunk = pd.DataFrame(rows)

    # Convert the DataFrame to an Arrow table
    table = pa.Table.from_pandas(df_chunk)

    # Define the Parquet file name
    chunk_filename = f"output_chunk_{chunk_count}.parquet"

    # Write the Arrow table to a Parquet file
    pq.write_table(table, chunk_filename)
    print(f"Saved {chunk_filename} with {len(df_chunk)} rows.")

# Example usage

# Assuming reference_df is the DataFrame you reference for calculations
#reference_df = mediandf1
mediandf1 = pd.read_csv("mediandf1.csv")

# Generate condition pairs and element pairs
conditions = rmediandf1.index
elements = list(mediandf1.columns)

#condition_pairs   , assigned above with mediandf1
condition_pairs = list(itertools.combinations(mediandf1.index, 2))
element_pairs = list(itertools.combinations(elements, 2))

# Process all pairs in chunks and write the results to Parquet files
process_and_write_parquet(condition_pairs, element_pairs, mediandf1, chunk_size=10000)
