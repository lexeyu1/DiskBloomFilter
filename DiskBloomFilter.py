import math
import os
import struct
import xxhash

class DiskBloomFilter:
    def __init__(self, file_name, entries, error_rate):
        self._bit_per_entity = -math.log(error_rate) / (math.log(2) ** 2)
        self._bits = int(entries * self._bit_per_entity)
        self._bytes = (self._bits + 7) // 8  # Round up to the nearest byte
        self._hashes = int(math.ceil(math.log(2) * self._bit_per_entity))
        self._hash_seed = 0x32c1565a65b53543

        self.file_name = file_name
        if os.path.exists(file_name):
            with open(file_name, 'rb') as f:
                stored_entries, stored_error_rate = struct.unpack('ld', f.read(16))
                if entries != stored_entries:
                    raise ValueError(f"Stored entries {stored_entries} do not match provided {entries}")
                if error_rate != stored_error_rate:
                    raise ValueError(f"Stored error rate {stored_error_rate} does not match provided {error_rate}")
        else:
            with open(file_name, 'wb') as f:
                f.write(struct.pack('ld', entries, error_rate))
                f.write(b'\0' * self._bytes)

    def _get_bit_indices(self, data):
        hash_value = xxhash.xxh64(data, seed=self._hash_seed).intdigest()
        for i in range(self._hashes):
            yield (hash_value + i * hash_value) % self._bits

    def add(self, data):
        with open(self.file_name, 'r+b') as f:
            for bit_index in self._get_bit_indices(data):
                byte_index = bit_index // 8
                bit_offset = bit_index % 8
                f.seek(byte_index + 16)  # Offset by the size of entries and error_rate
                byte = f.read(1)[0]
                mask = 1 << bit_offset
                if byte & mask == 0:
                    f.seek(byte_index + 16)
                    f.write(bytes([byte | mask]))

    def check(self, data):
        with open(self.file_name, 'rb') as f:
            for bit_index in self._get_bit_indices(data):
                byte_index = bit_index // 8
                bit_offset = bit_index % 8
                f.seek(byte_index + 16)  # Offset by the size of entries and error_rate
                byte = f.read(1)[0]
                mask = 1 << bit_offset
                if byte & mask == 0:
                    return False
            return True

# Пример использования:
bloom_filter = DiskBloomFilter('bloom.bin', 1000000, 0.001)
bloom_filter.add(b'example data')
print(bloom_filter.check(b'example data'))  # Вернет True
print(bloom_filter.check(b'some other data'))  # Вероятно вернет False
