#!/bin/bash

# Variables
PYTHON_EXECUTABLE=$(which python3)  
DATABASE_PATH="./data/data_base.db"  
TEST_SCRIPT="./project/test_pipeline.py"  

debug_file_structure() {
    echo "Debugging file structure..."
    echo "Current working directory:"
    pwd
    echo "Listing all files and directories:"
    ls -R .
}

check_prerequisites() {
    echo "Checking prerequisites..."
    if [ ! -f "$TEST_SCRIPT" ]; then
        echo "Error: Test script '$TEST_SCRIPT' not found. Ensure the file exists in the correct path."
        exit 1
    fi

    if [ ! -f "$DATABASE_PATH" ]; then
        echo "Error: Database file '$DATABASE_PATH' not found. Run the pipeline before testing."
        exit 1
    fi
    echo "All prerequisites are met."
}

# Run tests
run_tests() {
    echo "Running tests..."
    $PYTHON_EXECUTABLE $TEST_SCRIPT
    if [ $? -eq 0 ]; then
        echo "All tests passed!"
    else
        echo "Some tests failed. Check the logs above."
        exit 1
    fi
}

# Main
debug_file_structure
check_prerequisites
run_tests