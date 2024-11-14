from collections import Counter
import csv
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
        lookup_table = {}
        

        with open(filepath, mode='r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                
                if str(row[0])+"_"+row[1] not in lookup_table:
                    lookup_table[str(row[0])+"_"+row[1]]=[row[2]]
                else:
                    set(lookup_table[str(row[0])+"_"+row[1]].append(row[2]))
        logging.info(f"Loaded lookup table from {filepath} with {len(lookup_table)} records.")
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
    for log in flow_logs:
        # Map the protocol number to its string name if it exists in protocol_mapping
        protocol = log.get('protocol')
        if isinstance(protocol, int) and protocol in protocol_mapping:
            log['protocol'] = protocol_mapping[protocol].lower()
        elif isinstance(protocol, str):
            log['protocol'] = protocol.lower()  # Ensure lowercase for string protocols
            
    logging.info("Protocols mapped successfully.")
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
    #flow_logs = map_protocols(flow_logs)
    
    # Ensure dstport and protocol columns are strings for consistent merging
    #flow_logs['dstport'] = flow_logs['dstport'].astype(str)
    #flow_logs['protocol'] = flow_logs['protocol'].astype(str)
    
    # Merge flow logs with lookup table on `dstport` and `protocol` columns
    for log in flow_logs:
        # Convert protocol to lowercase for consistency
        log['protocol'] = str(log['protocol']).lower()
        
        # Generate a combined key of dstport and protocol for lookup
        lookup_key = f"{log['dstport']}_{log['protocol']}"
        
        # Tagging process: check if the combined key is in the lookup table
        if lookup_key in lookup_table:
            log['tag'] = lookup_table[lookup_key]
        else:
            log['tag'] = ['Untagged']  # Default tag if no match found
    
    logging.info("Flow logs tagged successfully.")
    return flow_logs
        # if match:
        #     log['tag'] = match['tag']
        # else:
        #     log['tag'] = 'Untagged'  # Default tag if no match found

    # logging.info("Flow logs tagged successfully.")
    # return flow_logs

def count_tags(tagged_logs):
    """
    Count the occurrences of each tag in the tagged flow logs.
    Args:
        tagged_logs (DataFrame): A DataFrame containing tagged flow log data.
    Returns:
        DataFrame: A DataFrame with tag counts.
    """
    try:
        # Extract tags from each log entry
        #tags = [log['tag'] for log in tagged_logs if 'tag' in log]
        tags=[]
        for log in tagged_logs:
            #print("log:",log)
            if 'tag' in log:
                for tag in log['tag']:
                    tags.append(tag)
        # Count occurrences of each tag using Counter
        tag_counts = Counter(tags)
        
        # Convert the counter to a list of dictionaries with 'Tag' and 'Count' keys
        tag_counts_list = [{'Tag': tag, 'Count': count} for tag, count in tag_counts.items()]
        
        logging.info("Successfully counted tags.")
        return tag_counts_list
    except KeyError:
        logging.error("The 'tag' field is missing in one or more entries in tagged logs.")
        raise
    except Exception as e:
        logging.error(f"An error occurred while counting tags: {e}")
        raise

def save_tag_counts(tag_counts, output_filepath):
    """
    Save the tag counts to a CSV file.
    Args:
        tag_counts (DataFrame): A DataFrame with tag counts.
        output_filepath (str): Path to save the output CSV file.
    """
    try:
        # Write tag counts to CSV
        with open(output_filepath, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['Tag', 'Count'])
            writer.writeheader()  # Write the header
            writer.writerows(tag_counts)  # Write the data rows
            
        logging.info(f"Tag counts saved to {output_filepath}")
    except Exception as e:
        logging.error(f"Failed to save tag counts to file: {e}")
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
    column_names = ["version", "account_id", "interface_id", "srcaddr", "dstaddr", 
                    "dstport", "srcport", "protocol", "packets", "bytes", 
                    "start", "end", "action", "log_status"]
    chunk_list = []

    with open(flow_logs_file_path, mode='r') as file:
        csv_reader = csv.reader(file, delimiter=' ')
        current_chunk = []
        chunk_size = 100000
        for row in csv_reader:
            # Filter out any empty strings caused by multiple spaces
            row = [value for value in row if value]
            
            # Convert each row into a dictionary using column names
            if len(row) == len(column_names):
                flow_log = {
                    column_names[i]: (
                        protocol_mapping[int(row[i])] if column_names[i] == "protocol" and int(row[i]) in protocol_mapping
                        else row[i]
                    ) for i in range(len(column_names))
                }
                current_chunk.append(flow_log)
            # When chunk size is reached, process the chunk
            if len(current_chunk) >= chunk_size:
                tagged_chunk = tag_flow_logs(current_chunk, lookup_table)
                chunk_list.extend(tagged_chunk)
                current_chunk = []  # Reset the chunk

        # Process any remaining records in the final chunk
        if current_chunk:
            tagged_chunk = tag_flow_logs(current_chunk, lookup_table)
            chunk_list.extend(tagged_chunk)

    logging.info(f"Processed {len(chunk_list)} total flow logs.")
    return chunk_list

def count_port_protocol_combinations(tagged_logs, output_filepath):
    """
    Count occurrences of (dstport, protocol) combinations in tagged logs and save to a CSV file.
    
    Args:
        tagged_logs (list): List of dictionaries, each containing 'dstport' and 'protocol' keys.
        output_filepath (str): Path to save the CSV file.
    """
   
    protocol_map = {6: 'tcp', 1: 'icmp', 17: 'udp'}
    counts = Counter((str(log['dstport']), protocol_map.get(log['protocol'], str(log['protocol'])).lower())
                     for log in tagged_logs)

    port_protocol_counts = [{'dstport': dstport, 'protocol': protocol, 'count': count} 
                            for (dstport, protocol), count in counts.items()]

    try:
        with open(output_filepath, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['dstport', 'protocol', 'count'])
            writer.writeheader()
            writer.writerows(port_protocol_counts)
        
        logging.info(f"Port-protocol counts saved to {output_filepath}")
    except Exception as e:
        logging.error(f"Failed to save port-protocol counts to file: {e}")
        raise


