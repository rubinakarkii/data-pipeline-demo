# Data Pipeline Demo
# Project Setup

## Technologies Used

    Python: 3.10.15
    PySpark: 3.5.3
    MongoDB: 8.0.1

**NOTES: Since PySpark is used in the project, make sure Java (version 17.0.13) is installed on your system.**

For macOS, *brew install openjdk@17*  

## Activate the Virtual Environment

Navigate to the project directory and create a virtual environment:

    cd data-pipeline-demo
    python3.10 -m venv myenv
    source myenv/bin/activate

## Install Project Dependencies

Install all the required packages by running:

    pip install -r requirements.txt

## Set Up Environment Variables

Create a *.env* file by following the structure in *env_example* file. This file should include any necessary environment variables for the project.

## Run the Data Pipeline
Execute the pipeline with the following command:

    python components/run_pipeline.py

Ensure that the virtual environment is activated before running the command.