import os
import logging
from src import flow_log_process as fp
import config

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def main():
    # Define file paths from config.py
    lookup_file_path = config.LOOKUP_FILE_PATH
    flow_logs_file_path = config.FLOW_LOGS_FILE_PATH
    output_file_path = config.OUTPUT_FILE_PATH
    port_protocol_file_path = config.PORT_PROTOCOL_FILE_PATH
    
    try:
        # Load lookup table
        logger.info("Loading lookup table...")
        lookup_table = fp.load_lookup_table(lookup_file_path)
        logger.info("Lookup table loaded successfully.")
        
        # Load and tag flow logs in chunks
        logger.info("Processing flow logs in chunks...")
        tagged_logs = fp.process_logs_in_chunks(flow_logs_file_path, lookup_table)
        logger.info(f"Processed {len(tagged_logs)} flow log entries.")

        # Count tags
        logger.info("Counting tags...")
        tag_counts = fp.count_tags(tagged_logs)

        # Save the tag counts to CSV
        logger.info("Saving tag counts to CSV...")
        fp.save_tag_counts(tag_counts, output_file_path)
        logger.info(f"Tag counts saved to {output_file_path}.")
        
        # Optional: Generate additional summary stats (e.g., protocol counts)
        logger.info("Generating port-protocol counts...")
        #port_protocol_counts = tagged_logs.groupby(['dstport', 'protocol']).size().reset_index(name='count')
        fp.count_port_protocol_combinations(tagged_logs,port_protocol_file_path)
        # Map protocol numbers to names (e.g., 6 -> tcp)
       # protocol_map = {6: 'tcp', 1: 'icmp', 17: 'udp'}
        #port_protocol_counts['protocol'] = port_protocol_counts['protocol'].map(protocol_map)

        # Save port-protocol count data to CSV
        #logger.info(f"Saving port-protocol counts to {port_protocol_file_path}...")
        #port_protocol_counts.to_csv(port_protocol_file_path, index=False, header=True)
        logger.info(f"Port-protocol counts saved to {port_protocol_file_path}.")
    
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
