# main.py
from typing import List
from tqdm import tqdm
from private_key_generator import generate_private_keys_cython, generate_wallet_info_cython
from database_operations import store_wallet_info_cython

# Your existing code for imports and database configuration
DB_NAME = 'wall_info'
DB_USER = 'postgres'
DB_PASSWORD = 'v4OOK0ZCA3K6!'
DB_HOST = '192.168.0.75'
DB_PORT = '5432'

# Connect to the database
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

# Set up logging
logging.basicConfig(filename='pgwallet.log', level=logging.INFO)

# Create a connection pool
pool = SimpleConnectionPool(1, 20, user=DB_USER,
                            password=DB_PASSWORD,
                            host=DB_HOST,
                            port=DB_PORT,
                            database=DB_NAME)
def process_chunk(chunk: List[int], start_hex: str):
    private_keys = generate_private_keys_cython(start_hex, len(chunk))
    wallet_info_list = [generate_wallet_info_cython(private_key) for private_key in private_keys]
    store_wallet_info_cython(wallet_info_list)

def store_wallet_info(wallet_info_list: List[dict]):
    conn = pool.getconn()
    try:
        with conn.cursor() as cursor:
            # Create a temporary table for bulk insertions
            cursor.execute('''CREATE TABLE IF NOT EXISTS wallet_info (
                                private_key_hex TEXT,
                                private_key_wif TEXT,
                                private_key_wif_compressed TEXT,
                                public_key TEXT,
                                public_key_compressed TEXT,
                                public_address_1 TEXT,
                                public_address_1_compressed TEXT,
                                public_address_3 TEXT,
                                public_address_bc1_P2WPKH TEXT,
                                public_address_bc1_P2WSH TEXT
                            )''')

            # Insert wallet info into the database
            cursor.executemany('''INSERT INTO wallet_info (
                                        private_key_hex,
                                        private_key_wif,
                                        private_key_wif_compressed,
                                        public_key,
                                        public_key_compressed,
                                        public_address_1,
                                        public_address_1_compressed,
                                        public_address_3,
                                        public_address_bc1_P2WPKH,
                                        public_address_bc1_P2WSH
                                    ) VALUES (%(private_key_hex)s, %(private_key_wif)s, %(private_key_wif_compressed)s,
                                              %(public_key)s, %(public_key_compressed)s, %(public_address_1)s,
                                              %(public_address_1_compressed)s, %(public_address_3)s,
                                              %(public_address_bc1_P2WPKH)s, %(public_address_bc1_P2WSH)s)''', wallet_info_list)
            conn.commit()
            logging.info("Batch stored successfully")  # Print success message

    except Exception as e:
        logging.error(f"Error storing wallet info: {e}")
        logging.error("Error storing batch")  # Print error message

    finally:
        pool.putconn(conn)

def main():
    total_keys = 1000000000000  # Set the total number of generated keys
    chunk_size = 10000  # Set the desired chunk size

    generate_method = input("Do you want to generate keys randomly? (yes/no): ")
    if generate_method.lower() == "yes":
        start_hex = None  # Generate keys randomly
    else:
        start_hex = input("Enter the starting hex key: ")

    # Initialize the progress bar
    pbar = tqdm.tqdm(total=total_keys, position=0, leave=True)

    # Process the chunks using @jit-optimized function
    for i in range(0, total_keys, chunk_size):
        process_chunk(list(range(i, i + chunk_size)), start_hex)
        pbar.update(chunk_size)

    pbar.close()

if __name__ == "__main__":
    main()