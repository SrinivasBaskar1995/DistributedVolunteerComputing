# Distributed Volunteer Computing

Performing deep learning analysis on Video Stream using Volunteer Computing.
Volunteer computing is a form of distributed computing where every client can join and leave at will and request processing. Each client will contribute a certain amount of its resources.

This project contains

--server.py - run server for distribution and management. Provide the ip address of the server as a command line argument.

--worker.py - run client nodes on user devices. Provide ip addresses of server and the client as a command line argument in that order.

Every client starts as a worker. When the user needs to request processing, type request in the terminal. When you want to finish processing(in case of live stream) type end in the terminal. Type quit to leave.

Commands on client node:

1. request live --> for requesting live stream processing
2. request path-to-video --> for requesting video stream processing
3. end --> to stop processing
4. quit --> to leave

Commands on server node:

1. quit --> to leave
