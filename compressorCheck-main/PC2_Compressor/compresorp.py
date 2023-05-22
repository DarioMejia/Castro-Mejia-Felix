import heapq
import pickle
import sys
import time
from mpi4py import MPI


class HuffmanNode:
    def __init__(self, char, freq):
        self.char = char
        self.freq = freq
        self.left = None
        self.right = None

    def __lt__(self, other):
        return self.freq < other.freq

    def __eq__(self, other):
        return self.freq == other.freq


def build_huffman_tree(freq_map):
    # Create a priority queue with nodes representing each character and their frequency
    heap = [HuffmanNode(char, freq) for char, freq in freq_map.items()]
    heapq.heapify(heap)

    # Build the Huffman tree
    while len(heap) > 1:
        min1 = heapq.heappop(heap)
        min2 = heapq.heappop(heap)
        combined = HuffmanNode(None, min1.freq + min2.freq)
        combined.left = min1
        combined.right = min2
        heapq.heappush(heap, combined)

    return heap[0]


def build_code_map(node, current_code, code_map):
    if node is None:
        return

    if node.char is not None:
        code_map[node.char] = current_code

    build_code_map(node.left, current_code + "0", code_map)
    build_code_map(node.right, current_code + "1", code_map)


def frequency(start,end,content):
    freq_map = {}
    for char in content[start:end]:
        if char not in freq_map:
            freq_map[char] = 0
        freq_map[char] += 1
    return freq_map

def encode(start,end,content,code_map):
    return "".join([code_map[char] for char in content[start:end]])

if __name__ == "__main__":
    # Declaraciones para la paralelización
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    data = [0]* 4 

    # Proceso 0 es administrador y trabajador
    if rank==0:
        process = {}
        total_time = 0
 
        #Lectura del archivo
        original_file = sys.argv[1]
        compressed_file = "comprimido.elmejorprofesor"
        #Inicio del proceso de compresion

        start_time = time.time()
        # Read input file
        with open(original_file, 'rb') as file:
         content = file.read().decode('ISO-8859-1')
        end_time = time.time()
        total_time += end_time - start_time
        process["Read file"] = end_time - start_time

        #Asignar contenido a cada proceso
        start_time = time.time()
        freq_maps = []
        for i in range(1,size):
            parte_size = len(content) // size
            data[0] = i * parte_size
            data[1] = (i + 1) * parte_size
            data[2] = content
            comm.ssend(data, dest=i)

        #trabajar el mismo
        parte_size = len(content) // size
        data[0] = rank * parte_size
        data[1] = (rank + 1) * parte_size
        data[2] = content
        freque = frequency(data[0],data[1],data[2])
        #print(freque,"PROCESO", rank,"\n")
        freq_maps = comm.gather(freque, root=0)   
        combined_freq_map = {}
        for freq_map in freq_maps:
            for char, count in freq_map.items():
                combined_freq_map[char] = combined_freq_map.get(char, 0) + count
        end_time = time.time()
        total_time += end_time - start_time
        process["Frequency map"] = end_time - start_time

        #print(combined_freq_map)
        #Hasta aquí tendríamos la función de frecuencia paralelizada
        start_time = time.time()
        # Build Huffman tree
        root = build_huffman_tree(combined_freq_map)
        end_time = time.time()
        total_time += end_time - start_time
        process["Build Huffman tree"] = end_time - start_time
        
        start_time = time.time()
        # Generate code map
        code_map = {}
        build_code_map(root, "", code_map)
        end_time = time.time()
        total_time += end_time - start_time
        process["Generate code map"] = end_time - start_time
        
        # Encode content
        start_time = time.time()
        encoded_content = "".join([code_map[char] for char in content])
        end_time = time.time()
        total_time += end_time - start_time
        process["encoded"] = end_time - start_time

        start_time = time.time()
        # Convert the encoded content string to a bytearray
        padded_encoded_content = encoded_content + "1"
        padding_length = 8 - len(padded_encoded_content) % 8
        padded_encoded_content += "0" * padding_length
        end_time = time.time()
        total_time += end_time - start_time
        process["padding"] = end_time - start_time

        start_time = time.time()
        byte_array = bytearray([int(padded_encoded_content[i:i + 8], 2) for i in range(0, len(padded_encoded_content), 8)])
        end_time = time.time()
        total_time += end_time - start_time
        process["To bytearray"] = end_time - start_time

        start_time = time.time()
        # Save compressed data
        with open(compressed_file, "wb") as file:
            data = (combined_freq_map, padding_length, byte_array)
            pickle.dump(data, file)
        end_time = time.time()
        total_time += end_time - start_time
        process["save the file"] = end_time - start_time

        print(f"Compression time: {total_time:.2f}")

        for key, item in process.items():
            print(f" - {key} {item:.2f} {(item/total_time*100):.2f}%")
    else:
        data = comm.recv(source=0)
        freque = frequency(data[0],data[1],data[2])
        #print(freque,"PROCESO", rank,"\n")
        freq_maps = comm.gather(freque, root=0)
