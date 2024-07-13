import logging

# Clear the log file
with open('log_file.log', 'w'):
    pass

logging.basicConfig(filename='log_file.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
