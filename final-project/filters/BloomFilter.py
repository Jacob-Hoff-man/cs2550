from Common import Filter
import math
import mmh3

# reference: https://techtonics.medium.com/implementing-bloom-filters-in-python-and-understanding-its-error-probability-a-step-by-step-guide-13c6cb2e05b7
class BloomFilter(Filter):
    '''
    Python implementation of Bloom filters
    '''
    def __init__(self, capacity = 512, num_bits = 6716, error_rate = 0.02, num_hashes = 2):
        '''
        Initializes a Bloom filter with a given capacity and error rate
        '''
        self.capacity = capacity # maximum number of elements that can be stored
        self.error_rate = error_rate # desired maximum false positive rate
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        
        # TODO: repair and make the input params optional
        # if num_hashes == None:
        #     self.num_bits = self.get_min_num_bits(capacity, error_rate) # number of bits needed
        #     self.num_hashes = self.get_num_hashes(self.num_bits, capacity) # number of hash functions needed
        # else:
        #     self.num_bits = self.get_num_bits(capacity, error_rate, num_hashes) # number of bits needed
        #     self.num_hashes = num_hashes
        
        self.bit_array = [0] * self.num_bits # bit array to store the elements

        print('@@init bloom filter', self.num_hashes, self.num_bits)
        
    def add(self, key):
        for i in range(self.num_hashes):
            # generates a hash value for the element and sets the corresponding bit to 1
            hash_val = mmh3.hash(key, i) % self.num_bits
            self.bit_array[hash_val] = 1
        
    def __contains__(self, key):
        for i in range(self.num_hashes):
            # generates a hash value for the element and checks if the corresponding bit is 1
            hash_val = mmh3.hash(key, i) % self.num_bits
            if self.bit_array[hash_val] == 0:
                # if any of the bits is 0, the element is definitely not present
                return False
        # if all the bits are 1, the element may or may not be present
        return True
    
    def get_num_bits(self, capacity, error_rate, num_hashes):
        '''
        Calculates the number of bits for a given capacity, error rate, and hash function count
        '''
        # TODO: does not work
        num_bits = 1/(1-math.pow(-(math.pow(error_rate, 1/num_hashes)-1), 1/num_hashes*capacity))
        return num_bits
        

    def get_min_num_bits(self, capacity, error_rate):
        '''
        Calculates the minimum number of bits needed for a given capacity and error rate
        '''
        num_bits = - (capacity * math.log(error_rate)) / (math.log(2) ** 2)
        return int(num_bits)
    
    def get_num_hashes(self, num_bits, capacity):
        '''
        Calculates the number of hash functions needed for a given number of bits and capacity
        '''
        num_hashes = (num_bits / capacity) * math.log(2)
        return int(num_hashes)