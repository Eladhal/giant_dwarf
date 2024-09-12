import json
import multiprocessing as mp

from constants import DATA_LOCATION, CHUNK_SIZE
from utils import get_chunks, remove_duplications_in_list_of_obj


class DwarfGiant:
    def __init__(self):
        self.unique_list_employees = []
        self._pairs = []
        self.init_data()

    @property
    def pairs(self):
        return self._pairs

    def init_data(self):
        self.load_data()

    def load_data(self):
        with open(DATA_LOCATION, 'r') as file:
            list_employees = json.load(file)

        list_of_chunks_of_employees = list(get_chunks(list_employees, CHUNK_SIZE))
        self.process_unique_list_in_chunks_multiprocess(list_of_chunks_of_employees)

    def process_unique_list_in_chunks_multiprocess(self, list_of_chunks_of_employees):
        with mp.Pool(processes=mp.cpu_count()) as pool:  # Set up a pool of workers
            results = pool.map(remove_duplications_in_list_of_obj, list_of_chunks_of_employees)

        result_set = set().union(*results)  # Combine all sets from the results into one big set
        self.unique_list_employees = [dict(t) for t in result_set]

    def create_pairs(self):
        list_of_chunks_of_employees = list(get_chunks(self.unique_list_employees, CHUNK_SIZE))
        with mp.Pool(processes=mp.cpu_count()) as pool:  # Set up a pool of workers
            results = pool.map(self.handle_chunk, list_of_chunks_of_employees)

        flattened_results = [item for chunk in results for item in chunk] # Final flat list of employees after processing
        self._pairs = flattened_results

    def handle_chunk(self, chunk):
        pairs = []
        for index in range(0, len(chunk) - 1):
            pairs.append((chunk[index]['name'], chunk[index + 1]['name']))
        pairs.append((chunk[len(chunk) - 1]['name'], chunk[0]['name']))
        return pairs


if __name__ == '__main__':
    game = DwarfGiant()
    game.create_pairs()
    print(game.pairs)
