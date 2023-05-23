import time
import pickle
import sys
from mpi4py import MPI
from compresor import build_huffman_tree

def decodeTree(encoded_content_part, Root):
    current_node = Root
    decoded_content = []
    for bit in encoded_content_part:
        if bit == "0":
            current_node = current_node.left
        else:
            current_node = current_node.right
        if current_node.char is not None:
            decoded_content.append(current_node.char.encode('ISO-8859-1'))
            current_node = Root
    return decoded_content

if __name__ == "__main__":
    datos = [0]* 3 
    compressed_file = sys.argv[1]
    decompressed_file = "descomprimidop-elmejorprofesor.txt"
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    total_time = 0
    start_time = time.time()
    # Load compressed data
    with open(compressed_file, "rb") as file:
        data = pickle.load(file)


    if rank == 0:
        freq_map, padding_length, byte_array = data


         # Rebuild Huffman tree
        Root = build_huffman_tree(freq_map)

        # Convert the bytearray to an encoded content string

        # Determina el tamaño de las partes del byte_array a procesar
        parte_size = len(byte_array) // size
        for i in range(1,size):
            if i == size - 1:
                byte_array_part = byte_array[i * parte_size:]
            else:
                byte_array_part = byte_array[i * parte_size:(i + 1) * parte_size]
            comm.send(byte_array_part, dest=i)
        byte_array_part = byte_array[rank * parte_size:(rank + 1) * parte_size]
        # Convierte el bytearray a partes de la cadena de contenido codificado
        encoded_content_parte = "".join([format(byte, "08b") for byte in byte_array_part])

        # Recolecta los resultados de todos los procesos
        encoded_content_parts = comm.gather(encoded_content_parte, root=0)
        encoded_content = "".join(encoded_content_parts)
        encoded_content = encoded_content[:-(padding_length + 1)]

        # Decode content
        part_starts = [0]
        part_ends = []

        for i in range(size - 1):
            current_pos = part_starts[-1]
            while current_pos < len(encoded_content):
                current_node = Root
                while current_node.char is None:
                    current_bit = encoded_content[current_pos]
                    current_node = current_node.left if current_bit == "0" else current_node.right
                    current_pos += 1
                part_ends.append(current_pos)
                if len(part_ends) == i + 1:
                    break
            part_starts.append(current_pos)
        part_ends.append(len(encoded_content))


        for i in range(1, size):
            start = part_starts[i]
            end = part_ends[i]
            datos[0] = encoded_content[start:end]
            datos[1] = Root
            comm.send(datos, dest=i)
        decode_part = decodeTree(encoded_content[part_starts[0]:part_ends[0]], Root)
        decodes_parts = comm.gather(decode_part, root=0)
        decoded_content = []
        for i in decodes_parts:
            decoded_content += i

        
        # Write decoded content
        with open(decompressed_file, "wb") as file:
            file.write(b''.join(decoded_content))


        end_time = time.time()
        total_time += end_time - start_time
        print(f" {total_time:.2f} ")


    else:
        parte = comm.recv(source=0)
        encoded_content_parte = "".join([format(byte, "08b") for byte in parte])
        comm.gather(encoded_content_parte, root=0)
        datos = comm.recv(source=0)
        decode_part = decodeTree(datos[0],datos[1])
        comm.gather(decode_part, root=0)


