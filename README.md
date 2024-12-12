# wikipedia-clickstream
A Clickstream Analysis Tool for wikipedia


## MVP
- download and send data to kafka 
    - fetch all dates
    - fetch all updated dates 
    - downloads the gzip file
    - extracts it
    - sends it to kafka 
- send kafka data to spark
- process spark and store in cassandra
- show cassandra data via visuals 


## Extras 
- host on azure 
- try multiple spark instances as workers
- mock clickstream 

