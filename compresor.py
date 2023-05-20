import heapq
import pickle
import os
import sys
import time

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


def compress(original_file, compressed_file):
    # Read input file
    with open(original_file, 'rb') as file:
        content = file.read().decode('ISO-8859-1')

    # Build frequency map
    freq_map = {}
    for char in content:
        if char not in freq_map:
            freq_map[char] = 0
        freq_map[char] += 1

    # Build Huffman tree
    root = build_huffman_tree(freq_map)

    # Generate code map
    code_map = {}
    build_code_map(root, "", code_map)

    # Encode content
    encoded_content = "".join([code_map[char] for char in content])

    # Convert the encoded content string to a bytearray
    padded_encoded_content = encoded_content + "1"
    padding_length = 8 - len(padded_encoded_content) % 8
    padded_encoded_content += "0" * padding_length
    byte_array = bytearray([int(padded_encoded_content[i:i + 8], 2) for i in range(0, len(padded_encoded_content), 8)])

    # Save compressed data
    with open(compressed_file, "wb") as file:
        data = (freq_map, padding_length, byte_array)
        pickle.dump(data, file)


if __name__ == "__main__":
    original_file = sys.argv[1]
    compressed_file = "comprimido.elmejorprofesor"

    start_time = time.time()
    compress(original_file, compressed_file)
    end_time = time.time()

    print(f"Compression time: {end_time - start_time:.2f} seconds")
    print(f"Original file size: {os.path.getsize(original_file)} bytes")
    print(f"Compressed file size: {os.path.getsize(compressed_file)} bytes")