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
#include "relay_client.h"

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

using std::string;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using helloworld::HelloRequest;
using helloworld::HelloReply;
using helloworld::Greeter;

void RelayClient::RelayHello(RelayState* relayState) {
    // Data we are sending to the server.
    HelloRequest request;
    request.set_name(relayState->request.name());

    // Call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;
    call->relayState_ = relayState;

    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->response_reader =
        stub_->PrepareAsyncSayHello(&call->context, request, &cq_);

    // StartCall initiates the RPC call
    call->response_reader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->response_reader->Finish(&call->reply, &call->status, (void*)call);
}

// Loop while listening for completed responses.
// Prints out the response from the server.
void RelayClient::AsyncCompleteRpc() {
    void* got_tag;
    bool ok = false;

    // Block until the next result is available in the completion queue "cq".
    while (cq_.Next(&got_tag, &ok)) {
        // The tag in this example is the memory location of the call object
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

        // Verify that the request was completed successfully. Note that "ok"
        // corresponds solely to the request for updates introduced by Finish().
        GPR_ASSERT(ok);

        if (call->status.ok()) {
            std::cout << "RelayClient received: " << call->reply.message() << std::endl;
            call->relayState_->serverReplay.set_message(call->reply.message());
        } else {
            static const string rpcErrMsg = "RPC failed";
            std::cout << rpcErrMsg << std::endl;
            call->relayState_->serverReplay.set_message(rpcErrMsg);
        }
        call->relayState_->FinishServerProcessing();
        // Once we're complete, deallocate the call object.
        delete call;
    }
}