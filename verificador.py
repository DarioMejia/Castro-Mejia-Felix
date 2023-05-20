import sys

def verify(original_file, decompressed_file):
    # Read original file
    with open(original_file, 'rb') as file:
        original_content = file.read().decode('ISO-8859-1')

    # Read decompressed file
    with open(decompressed_file, 'rb') as file:
        decompressed_content = file.read().decode('ISO-8859-1')

    # Verify that the original and decompressed contents are the same
    return "ok" if original_content == decompressed_content else "nok"


if __name__ == "__main__":
    original_file = sys.argv[1]
    decompressed_file = "descomprimido-elmejorprofesor.txt"

    result = verify(original_file, decompressed_file)
    print(f"Verification result: {result}")