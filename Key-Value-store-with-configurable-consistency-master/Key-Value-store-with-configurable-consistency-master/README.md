# Key-Value-store-with-configurable-consistency

1) Uses Google’s protocol buffers to implement a distributed key- value store which borrows designs from Dynamo and Cassandra.
2) This system puts the client provided data on a server using byte order partitioning along with back on different servers. The consistency level (ONE, QUORUM) can be chosen by the client. The same data can be retrieved from servers from any one of the servers which has a copy. While retrieving, the servers simultaneously checks for consistency and if not consistent, they use read-repair or hinted-handoff mechanisms as chosen by customer to get the correct data.
3) Uses Java and Google’s protocol buffers.


To Run:
make

./server.sh server1 replicas.txt 9091 hinted-handoff/read-repair

./client.sh server-ipAddress 9091
