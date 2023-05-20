import time
import pickle
from compresor import build_huffman_tree

def decompress(compressed_file, decompressed_file, freq_map, padding_length, byte_array):
    # Rebuild Huffman tree
    root = build_huffman_tree(freq_map)

    # Convert the bytearray to an encoded content string
    encoded_content = "".join([format(byte, "08b") for byte in byte_array])
    encoded_content = encoded_content[:-(padding_length + 1)]

    # Decode content
    current_node = root
    decoded_content = []
    for bit in encoded_content:
        if bit == "0":
            current_node = current_node.left
        else:
            current_node = current_node.right

        if current_node.char is not None:
            decoded_content.append(current_node.char.encode('ISO-8859-1'))
            current_node = root

    # Write decoded content
    with open(decompressed_file, "wb") as file:
        file.write(b''.join(decoded_content))


if __name__ == "__main__":
    compressed_file = "comprimido.elmejorprofesor"
    decompressed_file = "descomprimido-elmejorprofesor.txt"

    # Load compressed data
    with open(compressed_file, "rb") as file:
        data = pickle.load(file)

    freq_map, padding_length, byte_array = data

    start_time = time.time()
    decompress(compressed_file, decompressed_file, freq_map, padding_length, byte_array)
    end_time = time.time()

    print(f"Decompression time: {end_time - start_time:.2f} seconds")