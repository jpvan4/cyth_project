import logging 
import psycopg2 
import psycopg2.extras 
cimport psycopg2
cimport psycopg2.extensions
cimport cython
from libc.stdlib cimport malloc, free
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t
cimport self 


DB_NAME = 'wall_info' 
DB_USER = 'postgres' 
DB_PASSWORD = 'v4OOK0ZCA3K6!' 
DB_HOST = '192.168.0.75' 
DB_PORT = '5432' 

# Declare the types of conn and cursor variables 
cdef class Psycopg2Extensions: 
    cdef psycopg2.extensions.connection conn 
    cdef psycopg2.extensions.cursor cursor 
    def __cinit__(self): 
        self.conn = None 
        self.cursor = None 
    cpdef connect(self, str dbname, str user, str password, str host, str port): 
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port) 
    cpdef self.cursor(self): 
        self.cursor = self.conn.cursor() 
        return self.cursor 
    cpdef commit(self): 
        self.conn.commit() 
    cpdef close(self): 
        if self.cursor is not None: 
            self.cursor.close() 
        if self.conn is not None: 
            self.conn.close() 

cpdef store_wallet_info_cython(list wallet_info_list, Psycopg2Extensions ext): 
    cdef self.psycopg2.extensions.Cursor cursor 
    try: 
        ext.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT) 
        cursor = ext.cursor() 
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
        psycopg2.extras.execute_values(cursor, '''INSERT INTO wallet_info ( 
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
                                    ) VALUES %s''', wallet_info_list) 
        ext.commit() 
        logging.info("Batch stored successfully")  # Print success message 
    except Exception as e: 
        logging.error(f"Error storing wallet info: {e}") 
        logging.error("Error storing batch")  # Print error message 
    finally: 
        ext.close()