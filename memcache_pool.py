import memcache
from time import sleep


class MemcacheSet:
    def __init__(self, pool_size: int, number_of_retry: int, timeout: int):
        self.pool = {}
        self.pool_size = pool_size
        self.number_of_retry = number_of_retry
        self.timeout = timeout

    def add_data(self, address: str, key: str, data: bytes)-> tuple:
        if address not in self.pool:
            client = memcache.Client([address])
            self.pool[address] = {}
            self.pool[address]['client'] = client
            self.pool[address]['data'] = {}
        self.pool[address]['data'][key] = data
        if len(self.pool[address]['data']) >= self.pool_size:
            number_processed, number_error = self.set_data(address)
        else:
            return 0, 0
        return number_processed, number_error

    def set_data(self, address: str) -> tuple:
        client = self.pool[address]['client']
        for _ in range(self.number_of_retry):
            errors = client.set_multi(self.pool[address]['data'])
            if not errors:
                number_processed = len(self.pool[address]['data'])
                number_error = 0
                self.pool[address]['data'].clear()
                return number_processed, number_error
            sleep(self.timeout)
        number_error = len(errors)
        number_processed = len(self.pool[address]['data']) - number_error
        return number_processed, number_error

    def final_send(self):
        number_processed, number_error = 0, 0
        for address in self.pool:
            number_processed_new, number_error_new = self.set_data(address)
            number_processed += number_processed_new
            number_error += number_error_new
        return number_processed, number_error
