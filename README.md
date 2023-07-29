# backpack.tf-ws-service

backpack.tf-ws-service is a script designed to connect to backpack.tf's websocket service and gather data. 

This data is then stored in a Cassandra database.

## Requirements
+ Python 3.10+
+ A running Cassandra database (I used DataStax Astra's)

## Installation

+ Clone the repository to your local machine:
    
    ```bash
    git clone git@github.com:purplebarber/backpack.tf-ws-service.git
    ```
+ Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```
+ Update the json file with your specific settings and preferences

+ Run the main.py script
    ```bash
    python main.py
    ```

