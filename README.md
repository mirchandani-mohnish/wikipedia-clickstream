# wikipedia-clickstream
A Clickstream Analysis Tool for wikipedia


## MVP
- download and send data to kafka 
    - fetch all dates
    - fetch all updated dates 
    - downloads the gzip file
    - extracts it
    - sends it to kafka 
    - integrate spark and make workers 
- send kafka data to spark 

- process spark and store in cassandra
    - read the particular word in cassandra 
    - update that in the same with increments to the particular column
- show cassandra data via visuals 


## Extras 
- host on azure 
    - data lake
    - cosmos db
    - s3 bucket alternative 
- try multiple spark instances as workers
- mock clickstream 







pip install cassandra-driver