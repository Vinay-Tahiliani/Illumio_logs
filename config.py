import os

# Define file paths for lookup tables, flow logs, and output files
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')

LOOKUP_FILE_PATH = os.path.join(DATA_DIR, 'lookup_table.csv')
FLOW_LOGS_FILE_PATH = os.path.join(DATA_DIR, 'logs.txt')
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, 'tag_counts.csv')
PORT_PROTOCOL_FILE_PATH = os.path.join(OUTPUT_DIR, 'port_protocol_counts.csv')

# Chunk size for reading logs
CHUNK_SIZE = 100000
