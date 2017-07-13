# EVE Exchange Utilities
These scripts are used to process all of the data used by EVE Exchange. The primary goal is to pull data from the EVE API as fast as possible, aggregate it, then push it to the frontend clients in near-realtime for visualization.

These scripts process market orders & related data, profit tracking for users, account management, and eve-mail.

## Usage
The scripts are written in Python 3.6.
An example crontab is included to demonstrate how often the scripts are meant to run.
Static data is required for knowing market item id's among other things. Currently static data is included for the Citadel release of EVE Online.

## Technology
MongoDB aggregation is the forefront of what makes these utilities possible. A significant amount of memory is required to operate smoothly, as well as SSD's recommended for reduced latency & increased throughput.

## Author
Christopher Hampu

## License
MIT
