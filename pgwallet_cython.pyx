# pgwallet_cython.pyx
from typing import List
from bitcoinaddress import Wallet
import secrets
import psycopg2
import psycopg2.extras
import multiprocessing
from psycopg2.pool import SimpleConnectionPool
from functools import partial
import time
import logging
import tqdm
import numpy as np
cimport cython

DB_NAME = 'wall_info'
DB_USER = 'postgres'
DB_PASSWORD = 'v4OOK0ZCA3K6!'
DB_HOST = '192.168.0.75'
DB_PORT = '5432'

# Connect to the database
pool = SimpleConnectionPool(1, 20, user=DB_USER,
                            password=DB_PASSWORD,
                            host=DB_HOST,
                            port=DB_PORT,
                            database=DB_NAME)

# Function to generate private keys and corresponding wallet info
def generate_private_keys(start_hex: str, num_keys: int) -> List[str]:
    if start_hex is None:
        return [secrets.token_hex(32) for _ in range(num_keys)]
    else:
        start_int = int(start_hex, 16)
        private_keys = []
        for i in range(num_keys):
            private_key_bytes = (start_int + i).to_bytes(32, byteorder='big')
            private_keys.append(private_key_bytes.hex())
        return private_keys

# Function to generate wallet info for a given private key
def generate_wallet_info(private_key: str) -> dict:
    wallet = Wallet(private_key)
    return {
        'private_key_hex': private_key,
        'private_key_wif': wallet.key.mainnet.wif,
        'private_key_wif_compressed': wallet.key.mainnet.wifc,
        'public_key': wallet.address.pubkey,
        'public_key_compressed': wallet.address.pubkeyc,
        'public_address_1': wallet.address.mainnet.pubaddr1,
        'public_address_1_compressed': wallet.address.mainnet.pubaddr1c,
        'public_address_3': wallet.address.mainnet.pubaddr3,
        'public_address_bc1_P2WPKH': wallet.address.mainnet.pubaddrbc1_P2WPKH,
        'public_address_bc1_P2WSH': wallet.address.mainnet.pubaddrbc1_P2WSH
    }

# Function to store wallet info in the database
def store_wallet_info(wallet_info_list: List[dict]):
    conn = pool.getconn()
    try:
        with conn.cursor() as cursor:
            # Create a temporary table for bulk insertions
            cursor.execute('''CREATE TABLE IF NOT EXISTS wallet_info (
                                private_key_hex VARCHAR,
                                private_key_wif VARCHAR,
                                private_key_wif_compressed VARCHAR,
                                public_key VARCHAR,
                                public_key_compressed VARCHAR,
                                public_address_1 VARCHAR,
                                public_address_1_compressed VARCHAR,
                                public_address_3 VARCHAR,
                                public_address_bc1_P2WPKH VARCHAR,
                                public_address_bc1_P2WSH VARCHAR
                            )''')
            # Insert wallet info into the database
            psycopg2.extras.execute_batch(cursor, '''INSERT INTO wallet_info (
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

def process_chunk(chunk: List[int], start_hex: str):
    private_keys = generate_private_keys(start_hex, len(chunk))
    wallet_info_list = [generate_wallet_info(private_key) for private_key in private_keys]
    store_wallet_info(wallet_info_list)

def main():
    total_keys = 1000000000000  # Set the total number of generated keys
    chunk_size = 3000000  # Set the desired chunk size
    generate_method = input("Do you want to generate keys randomly? (yes/no): ")
    if generate_method.lower() == "yes":
        start_hex = None  # Generate keys randomly
    else:
        start_hex = input("Enter the starting hex key: ")
    # Initialize the progress bar
    pbar = tqdm.tqdm(total=total_keys, position=0, leave=True)
    # Process the chunks using the process_chunk function defined here
    with multiprocessing.Pool() as pool:
        for i in range(0, total_keys, chunk_size):
            pool.apply_async(process_chunk, (list(range(i, i + chunk_size)), start_hex))
            pbar.update(chunk_size)
        pool.close()
        pool.join()
    pbar.close()

if __name__ == "__main__":
    main()