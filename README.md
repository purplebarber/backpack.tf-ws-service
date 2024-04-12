# backpack.tf-ws-service

backpack.tf-ws-service is a script designed to connect to backpack.tf's websocket service and gather listing data. 

It also calls the backpack.tf snapshot endpoint frequently to ensure the data is kept up to date.

This listings are stored in a MongoDB database.

## Requirements
+ Python 3.10+
+ A MongoDB instance running
+ A backpack.tf API token

## Installation

+ Clone the repository to your local machine:
    
    ```bash
    git clone git@github.com:purplebarber/backpack.tf-ws-service.git
    ```
+ Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```
+ Update the json file with the api token and mongo settings
  + Prioritized items are items that get their snapshots retrieved more frequently (useful if you are making a custom-pricer and want to keep the prices of certain items up to date)

+ Run the main.py script (ensure you have a mongodb instance running too):
    ```bash
    python main.py
    ```

