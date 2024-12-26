import heapq
import os
import sys

def read_numbers_and_divide(file_path, memory_limit):
    """ Divide input file with N numbers into N/M runs. """
    run = []
    current_bytes = 0
    NUMBER_SIZE = 40 # 40 bytes per number (integers in the file)

    with open(file_path, 'r') as file:
        for line in file:

            if current_bytes + NUMBER_SIZE > memory_limit: # if adding another line would extend memory M we dont add it and yield a run that is certainly under limit M
                if run:
                    yield run
                    run = []
                    current_bytes = 0

            number = int(line.strip())
            run.append(number)
            current_bytes += NUMBER_SIZE # dynamic calculation of a growing run, to ensure it does not extend our working memory M

        if run:
            yield run

def create_run_file(sorted_run, run_number):
    """ Create a text file containing a sorted run. The new file will be specified by run_number"""
    run_filename = f"run{run_number}.txt"
    with open(run_filename, 'w') as temp_file:
        for number in sorted_run:
            temp_file.write(f"{number}\n")
    return run_filename

def generate_runs(input_file_path, memory_limit):
    """ Sort N/M runs and create file for each. """
    runs = []
    index = 1
    for run in read_numbers_and_divide(input_file_path, memory_limit):
        
        run.sort() # Sorting in RAM
        run_file = create_run_file(run, index)
        runs.append(run_file) # storing paths to the files of the runs (In this case it is just a name of file for each run, we are working in one directory)
        index +=1 # index for unique name of each run
    return runs


def read_blocks(file_path, block_size):
    """ 
        Read a text file of run file  in blocks of size block_size (in bytes).
        - Ensures that each block contains complete lines (does not split lines).
        - If the block ends in the middle of a line, adjusts to include only full lines.
    """
    
    with open(file_path, 'r') as file:
        while True:
            block = file.read(block_size) # We only read a specified size in Bytes

            if not block:
                break

            # check if the block splitted a line, then we want to return to  start of the line (the splitted line will be processed in next block)
            # Might need optimalisation: if block read is one character before \n this function travels back to previous line, instead of including the current line

            if block[-1] != '\n': # The block jumped somewhere into the line
                last_newline = block.rfind('\n') # We look up the last \n character and only include lines up to this character
                file.seek(file.tell() - (len(block) - last_newline - 1))
                block = block[:last_newline + 1]
            yield block
  
def merge_K_runs(run_files, block_size, identifier):
    """ 
        Merge K sorted run files into a single sorted file.

        - run_files: List of file paths for K sorted runs.
        - block_size: Maximum size (in bytes) of a block to be read from each file.
        - identifier: Unique identifier for naming the output file.
        - Returns the name (path) of the merged output file.
    """
    readers, heap, current_blocks = [], [], [] # readers - generator

    for run_file in run_files: # for each of the K run files
        block = read_blocks(run_file, block_size) 
        readers.append(block)
    
    for index, reader in enumerate(readers):
        block = next(reader) # We load first block of size block_size  for each of the K run files
        lines = block.split("\n")
        lines = [int(line) for line in lines if line.strip()] # convert lines of the block into integers
        current_blocks.append(lines)
        heapq.heappush(heap, (current_blocks[-1].pop(0), index)) # we push the first number (the smallest) into the heap 
    # After this loop:
    # - current_blocks contains the first block of numbers for each of the K runs.
    # - heap contains the smallest element from each of these blocks,
    
    output_file = f"merged_run_{identifier}.txt" # output file is the merge of all K runs
    with open(output_file, 'w') as output:
        while heap: # While heap is not empty
            smallest, run_index = heapq.heappop(heap) # we pop the smallest element of the min heap and write it into output
            output.write(f"{smallest}\n")

            if current_blocks[run_index]: # If a number was written into output and it belonged into i-th run, we need to check if the block of i-th run has another number to be inserted into heap
                heapq.heappush(heap, (current_blocks[run_index].pop(0), run_index)) # if yes we push it to the heap and remove from the corresponding block
            else: # if we used all the numbers in the block, we need to load another block for the run
                try: # check if we can load another block or if we reached end of run file
                    new_block = next(readers[run_index])
                    lines = new_block.split('\n')
                    lines = [int(line) for line in lines if line.strip()]
                    current_blocks[run_index] = lines
                    
                    heapq.heappush(heap, (current_blocks[run_index].pop(0), run_index))

                except StopIteration:
                    pass

    return output_file # File containing sorted mergo of all K run files

def merge_runs(runs, output_file_path, memory_limit, block_size):
    """
    Iteratively merge sorted run files until a single sorted file remains.
    - runs: List of file paths for initial sorted runs.
    - output_file_path: Final output file path for the fully sorted data.
    - memory_limit: Maximum available memory (in bytes).
    - block_size: Size of each block to read from disk (in bytes).
    """
    K =  (memory_limit // block_size) # We can merge M/B files at once

    iteration = 1 # variable to name temporary files of merged runs

    while len(runs) > 1: # If we still have more than one run file we have to continue merging
        new_runs = []
        for i in range(0, len(runs), K): 
            current_runs = runs[i:i + K] # path to K run files
            merged_run = merge_K_runs(current_runs, block_size, f"{iteration}_{i // K + 1}")
            new_runs.append(merged_run) # new set of run files, that we get from merging
        
        for run in runs:
            os.remove(run) # We can delete all temp run files, since we have new merged files that we need

        iteration += 1
        runs = new_runs # new, reduced set of sorted run files 
    os.rename(runs[0], output_file_path) # The last remaining file is our sorted output


def external_merge_sort(input_file_path, memory_limit, block_size):
    """ Main function of external merge sort
        1. Divide the input file into smaller runs of size M (working memory limit). 
           Sort each run in memory and write it to a temporary file.
        2. Iteratively merge the runs until a single sorted output file remains.
    """
    first_limit = memory_limit // 3
    runs = generate_runs(input_file_path, first_limit)
    merge_runs(runs, "output2.txt", memory_limit, block_size)

def main():
    # CONSTANTS
 
    # MEMORY_LIMIT_BYTES = 100 * 1024 * 1024
    MEMORY_LIMIT_BYTES = 100 * 1024 * 1024
    # BLOCK_SIZE_BYTES = 512
    BLOCK_SIZE_BYTES = 512

    # Input file
    input_file = sys.argv[1]

    # External merge sort algorithm
    external_merge_sort(input_file, MEMORY_LIMIT_BYTES, BLOCK_SIZE_BYTES)

if __name__ == "__main__":
    main()

# TO RUN THE PROGRAM: -> python .\Rastocka_mergesort.py path_to_your_file.txt 