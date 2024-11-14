import pandas as pd
import logging
from pathlib import Path

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Mapping of protocol numbers to their string names
protocol_mapping = {
    6: 'tcp',
    17: 'udp',
    1: 'icmp'
}

def load_lookup_table(filepath):
    """
    Load the lookup table CSV file with dstport, protocol, and tag columns.
    Args:
        filepath (str): Path to the CSV file containing the lookup table.
    Returns:
        DataFrame: A pandas DataFrame with the lookup table data.
    """
    if not Path(filepath).is_file():
        logger.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"The specified file does not exist: {filepath}")
    
    try:
        # Read the lookup table and ensure case insensitivity for protocol matching
        column_names = ["dstport", "protocol", "tag"]
        lookup_table = pd.read_csv(filepath, header=None, names=column_names, dtype={'protocol': str})
        lookup_table['protocol'] = lookup_table['protocol'].str.lower()  # Convert protocol to lowercase
        logger.info(f"Loaded lookup table from {filepath} with {len(lookup_table)} records.")
        return lookup_table
    except Exception as e:
        logger.error(f"Failed to load lookup table: {e}")
        raise

def map_protocols(flow_logs):
    """
    Map protocol numbers in flow logs to string protocol names using the protocol_mapping.
    Args:
        flow_logs (DataFrame): Flow logs with numerical protocol values.
    Returns:
        DataFrame: Flow logs with mapped protocol names.
    """
    flow_logs['protocol'] = flow_logs['protocol'].map(protocol_mapping).fillna(flow_logs['protocol'])
    flow_logs['protocol'] = flow_logs['protocol'].str.lower()  # Convert to lowercase for case-insensitive comparison
    logger.info("Protocols mapped successfully.")
    return flow_logs

def tag_flow_logs(flow_logs, lookup_table):
    """
    Tag each flow log entry based on dstport and protocol.
    Args:
        flow_logs (DataFrame): DataFrame containing flow log data.
        lookup_table (DataFrame): DataFrame with the lookup table for tags.
    Returns:
        DataFrame: DataFrame with a 'tag' column added, where unmatched entries are labeled 'Untagged'.
    """
    # Map protocol numbers to strings
    flow_logs = map_protocols(flow_logs)
    
    # Ensure dstport and protocol columns are strings for consistent merging
    #flow_logs['dstport'] = flow_logs['dstport'].astype(str)
    #flow_logs['protocol'] = flow_logs['protocol'].astype(str)
    
    # Merge flow logs with lookup table on `dstport` and `protocol` columns
    tagged_logs = flow_logs.merge(lookup_table, on=['dstport', 'protocol'], how='left')
    tagged_logs['tag'].fillna('Untagged', inplace=True)
    
    logger.info("Flow logs tagged successfully.")
    return tagged_logs

def count_tags(tagged_logs):
    """
    Count the occurrences of each tag in the tagged flow logs.
    Args:
        tagged_logs (DataFrame): A DataFrame containing tagged flow log data.
    Returns:
        DataFrame: A DataFrame with tag counts.
    """
    try:
        tag_counts = tagged_logs['tag'].value_counts().reset_index()
        tag_counts.columns = ['Tag', 'Count']
        logger.info("Successfully counted tags.")
        return tag_counts
    except KeyError:
        logger.error("The 'tag' column is missing in the tagged logs DataFrame.")
        raise
    except Exception as e:
        logger.error(f"An error occurred while counting tags: {e}")
        raise

def save_tag_counts(tag_counts, output_filepath):
    """
    Save the tag counts to a CSV file.
    Args:
        tag_counts (DataFrame): A DataFrame with tag counts.
        output_filepath (str): Path to save the output CSV file.
    """
    try:
        tag_counts.to_csv(output_filepath, index=False)
        logger.info(f"Tag counts saved to {output_filepath}")
    except Exception as e:
        logger.error(f"Failed to save tag counts to file: {e}")
        raise

def process_logs_in_chunks(flow_logs_file_path, lookup_table):
    """
    Process flow logs in chunks to handle large files.
    Args:
        flow_logs_file_path (str): Path to the flow logs file.
        lookup_table (DataFrame): DataFrame containing the lookup table.
    Returns:
        DataFrame: Tagged flow logs.
    """
    chunk_size = 100000  # Define a chunk size suitable for your memory
    chunk_list = []
    
    for chunk in pd.read_csv(flow_logs_file_path, sep=r'\s+', header=None, chunksize=chunk_size):
        column_names = ["version", "account_id", "interface_id", "srcaddr", "dstaddr", 
                        "dstport", "srcport", "protocol", "packets", "bytes", 
                        "start", "end", "action", "log_status"]
        chunk.columns = column_names
        
        # Tag the flow logs in the current chunk
        tagged_chunk = tag_flow_logs(chunk, lookup_table)
        chunk_list.append(tagged_chunk)
    
    # Concatenate all the chunks into a single DataFrame
    tagged_logs = pd.concat(chunk_list, ignore_index=True)
    logger.info(f"Processed {len(tagged_logs)} total flow logs.")
    return tagged_logs


