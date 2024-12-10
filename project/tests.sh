#!/bin/bash

# Variables
PYTHON_EXECUTABLE=$(which python3)
DATABASE_PATH="./data/data_base.db"
PIPELINE_SCRIPT="./project/pipeline.py"
TEST_SCRIPT="./project/test_pipeline.py"

# Function to check prerequisites
check_prerequisites() {
    echo "Checking prerequisites..."
    
    # Ensure the pipeline script exists
    if [ ! -f "$PIPELINE_SCRIPT" ]; then
        echo "Error: Pipeline script '$PIPELINE_SCRIPT' not found."
        exit 1
    fi

    # Ensure the test script exists
    if [ ! -f "$TEST_SCRIPT" ]; then
        echo "Error: Test script '$TEST_SCRIPT' not found."
        exit 1
    fi

    echo "All prerequisites are met."
}

# Run the pipeline script to generate the database
run_pipeline() {
    echo "Running pipeline script to generate the database..."
    $PYTHON_EXECUTABLE $PIPELINE_SCRIPT
    if [ $? -eq 0 ]; then
        echo "Pipeline script executed successfully."
    else
        echo "Pipeline script failed to execute."
        exit 1
    fi

    # Check if the database file is created
    if [ ! -f "$DATABASE_PATH" ]; then
        echo "Error: Database file '$DATABASE_PATH' not found after running pipeline."
        exit 1
    fi
    echo "Database file created successfully."
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
check_prerequisites
run_pipeline
run_tests
