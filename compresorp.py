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

def baytepart(contentPart):
    return bytearray([int(contentPart[i:i + 8], 2) for i in range(0, len(contentPart), 8)])

if __name__ == "__main__":
    # Declaraciones para la paralelización
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    data = [0]* 5 
    
    #Lectura del archivo
    original_file = sys.argv[1]
    compressed_file = "comprimidop.elmejorprofesor"
    # Read input file
    with open(original_file, 'rb') as file:
        content = file.read().decode('ISO-8859-1')

    # Proceso 0 es administrador y trabajador
    if rank==0:

        total_time = 0
 
        #Inicio del proceso de compresion
        start_time = time.time()

        #Asignar contenido a cada proceso

        freq_maps = []
        for i in range(1,size):
            parte_size = len(content) // size
            data[0] = i * parte_size
            if i==size-1:
                data[1] = len(content)
            else:
                data[1] = (i + 1) * parte_size
            comm.send(data, dest=i)
        #trabajar el mismo
        parte_size = len(content) // size
        freque = frequency(0,parte_size,content)
        freq_maps = comm.gather(freque, root=0)   
        combined_freq_map = {}
        for freq_map in freq_maps:
            for char, count in freq_map.items():
                combined_freq_map[char] = combined_freq_map.get(char, 0) + count
        #Hasta aquí tendríamos la función de frecuencia paralelizada

        # Build Huffman tree
        root = build_huffman_tree(combined_freq_map)
   

        # Generate code map
        code_map = {}
        build_code_map(root, "", code_map)

        
        for i in range(1,size):
            data=code_map
            comm.send(data, dest=i)

        # Encode content
        encoded_content = encode(0,parte_size,content,code_map) #SUS(No me deja poner los data[i])
        parts_encoded_content = comm.gather(encoded_content, root=0)
        complete_encoded_content = ""
        for i in parts_encoded_content:
            complete_encoded_content += i

        # Convert the encoded content string to a bytearray
        padded_encoded_content = complete_encoded_content + "1"
        padding_length = 8 - len(padded_encoded_content) % 8
        padded_encoded_content += "0" * padding_length

        #paralelización de array de bytes

        parte_size_bits = len(padded_encoded_content) // size
        parte_size_bits -= parte_size_bits % 8
        for i in range(1, size):
            data[0] = i * parte_size_bits
            data[1] = data[0] + parte_size_bits
            if i == size - 1:
                data[1]= len(padded_encoded_content)
            part = padded_encoded_content[data[0]:data[1]]
            comm.send(part, dest=i)
        partbyte = baytepart(padded_encoded_content[0:parte_size_bits])

        # Recopila todas las partes convertidas en bytearray
        all_parts_bytes = comm.gather(partbyte, root=0)

        # Concatena todas las partes recopiladas en byte_array
        byte_array = bytearray()
        for part in all_parts_bytes:
            byte_array.extend(part)

        # Save compressed data
        with open(compressed_file, "wb") as file:
            data = (combined_freq_map, padding_length, byte_array)
            pickle.dump(data, file)
        end_time = time.time()
        total_time += end_time - start_time

        print(f" {total_time:.2f}")

    else:
        data = comm.recv(source=0)
        freque = frequency(data[0],data[1],content)
        freq_maps = comm.gather(freque, root=0)
        data[2] = comm.recv(source=0)
        encoded_content = encode(data[0],data[1],content,data[2])
        comm.gather(encoded_content, root=0)
        data[3] = comm.recv(source=0)
        partbyte=baytepart(data[3])
        comm.gather(partbyte, root=0)
