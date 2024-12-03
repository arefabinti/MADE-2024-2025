#!/bin/bash

# Variables
PYTHON_EXECUTABLE=$(which python3)  
DATABASE_PATH="../data/data_base.db"
TEST_SCRIPT="test_pipeline.py"

#Check prerequisites
check_prerequisites() {
    if [ ! -f "$TEST_SCRIPT" ]; then
        echo "Error: Test script '$TEST_SCRIPT' not found."
        exit 1
    fi

    if [ ! -f "$DATABASE_PATH" ]; then
        echo "Error: Database file '$DATABASE_PATH' not found. Run the pipeline before testing."
        exit 1
    fi
}

# Run tests
run_tests() {
    echo "--Running Tests--"
    $PYTHON_EXECUTABLE $TEST_SCRIPT
    if [ $? -eq 0 ]; then
        echo "Tests Passed"
    else
        echo "Tests Failed."
        exit 1
    fi
}

# Main
check_prerequisites
run_tests
