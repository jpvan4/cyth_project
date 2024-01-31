# private_key_generator.pyx
# Cython module for optimizing private key and wallet info generation

from typing import List
from bitcoinaddress import Wallet
import numpy as np
cimport numpy as np

@cython.boundscheck(False)
@cython.wraparound(False)
def generate_private_keys_cython(str start_hex, int num_keys) -> List[str]:
    cdef List[str] private_keys
    if start_hex is None:
        cdef List[bytes] private_key_bytes = np.random.bytes(num_keys * 32)
        private_keys = [private_key_byte.hex() for private_key_byte in private_key_bytes]
    else:
        cdef int start_int = int(start_hex, 16)
        private_keys = [((start_int + i).to_bytes(32, byteorder='big')).hex() for i in range(num_keys)]
    return private_keys

@cython.boundscheck(False)
@cython.wraparound(False)
def generate_wallet_info_cython(str private_key) -> dict:
    cdef Wallet wallet = Wallet(private_key)
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
