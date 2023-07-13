# Setup

## Step 1
Clone this repository to your local machine.

## Step 2
Copy the spark installation ( the `spark-3.2.3-bin-hadoop3.2` folder) into the clones repository. 

## Step 3
Create a virtual environment for the purposes of this training. Python version 3.9.13. Activate the environment.

## Step 4
Install the requirements for the requirements.txt:

`pip install -r requirements.txt`


## Step 5
Run the following command when inside the project directory to validate everything works:

`./spark-3.2.3-bin-hadoop3.2/bin/spark-submit src/test_spark.py`

You should see some logging output from spark with the text "SUCCESS!" at the end.
