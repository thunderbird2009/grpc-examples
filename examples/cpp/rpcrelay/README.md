RpcRelay
===================================

This example relays an gRPC call from client to server. The Relay service uses both async client and async server. Two worker threads
are used to handles tag from the client side completion queue and server side completion queue respectively. It uses the service protocol
buffer of the ../helloworld example.

A moore realistic scenario could be extended from this code base:
1. A pool of threads will be monitoring each completion queue.
2. The client-side completion queue will be used for multiple gRPC clients and callings of different server methods. Different client and
   methods will be distinguished by different tags returned by the completion queue Next() call.

# Compile and run

Compile the whole source tree of grpc with README at the root of the repository, which is a mirror of GRPC official repository. Make sure
that you can compile and run the Helloworld example. Then at this directory, type "make" to compile and link the binary.
Run it "./greeter_relay_server". You then need to start "greeter_server" and use "greeter_client" under ../helloworld to test the relay.
