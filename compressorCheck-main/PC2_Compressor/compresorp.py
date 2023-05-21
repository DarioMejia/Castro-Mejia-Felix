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
    #Declaraciones para la paralelización
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    data = [0]* 4 
    #Proceso 0 es administrador y trabajador
    if rank==0:
        freq_maps = []
        #Lectura del archivo
        original_file = sys.argv[1]
        compressed_file = "comprimido.elmejorprofesor"
        #Inicio del proceso de compresion
        start_time = time.time()

        # Read input file
        with open(original_file, 'rb') as file:
         content = file.read().decode('ISO-8859-1')
        
        #Asignar contenido a cada proceso
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
        #print(combined_freq_map)
        #Hasta aquí tendríamos la función de frecuencia paralelizada
        
        # Build Huffman tree
        root = build_huffman_tree(combined_freq_map)

        # Generate code map
        code_map = {}
        build_code_map(root, "", code_map)

        for i in range(1,size):
            data=code_map
            comm.ssend(data, dest=i)
        data[3]= code_map

        # Encode content
        encoded_content = encode(rank * parte_size,(rank + 1) * parte_size,content,code_map) #SUS(No me deja poner los data[i])
        parts_encoded_content = comm.gather(encoded_content, root=0)
        complete_encoded_content = ""
        for i in parts_encoded_content:
            complete_encoded_content = complete_encoded_content + i
        
        padded_encoded_content = complete_encoded_content + "1"
        padding_length = 8 - len(padded_encoded_content) % 8
        padded_encoded_content += "0" * padding_length
        byte_array = bytearray([int(padded_encoded_content[i:i + 8], 2) for i in range(0, len(padded_encoded_content), 8)])

        # Save compressed data
        with open(compressed_file, "wb") as file:
            data = (combined_freq_map, padding_length, byte_array)
            pickle.dump(data, file)
        
        end_time = time.time()

        print(f"Compression time: {end_time - start_time:.2f} seconds")
    else:
        data = comm.recv(source=0)
        freque = frequency(data[0],data[1],data[2])
        #print(freque,"PROCESO", rank,"\n")
        freq_maps = comm.gather(freque, root=0)
        data[3] = comm.recv(source=0)
        encoded_content = encode(data[0],data[1],data[2],data[3])
        comm.gather(encoded_content, root=0)




