/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include <thread>

#include "helloworld.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;

class RelayState {
  public:
    HelloRequest request;
    HelloReply serverReplay;

    // All server side processing is finished. Call it to start to responding
    // to client.
    virtual void FinishServerProcessing() = 0;
};

class RelayClient {
  public:
    explicit RelayClient(std::shared_ptr<Channel> channel)
            : stub_(Greeter::NewStub(channel)) {};
    
    // Assembles the client's payload and sends it to the server.
    void RelayHello(RelayState* relayState);

    // Loop while listening for completed responses.
    // Prints out the response from the server.
    void AsyncCompleteRpc();

  private:
    // struct for keeping state and data information
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        HelloReply reply;

        RelayState* relayState_;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;


        std::unique_ptr<ClientAsyncResponseReader<HelloReply>> response_reader;
    };

    // Out of the passed in Channel comes the stub, stored here, our view of the
    // server's exposed services.
    std::unique_ptr<Greeter::Stub> stub_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;
};