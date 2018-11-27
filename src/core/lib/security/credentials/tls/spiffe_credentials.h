/*
 *
 * Copyright 2018 gRPC authors.
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

#ifndef GRPC_CORE_LIB_SECURITY_CREDENTIALS_TLS_SPIFFE_CREDENTIALS_H
#define GRPC_CORE_LIB_SECURITY_CREDENTIALS_TLS_SPIFFE_CREDENTIALS_H

#include <grpc/support/port_platform.h>

#include <grpc/grpc_security.h>

#include "src/core/lib/security/credentials/credentials.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_credentials_options.h"

/* Main struct for grpc SPIFFE channel credential. */
typedef struct grpc_tls_spiffe_credentials {
  grpc_channel_credentials base;
  grpc_tls_credentials_options* options;
} grpc_tls_spiffe_credentials;

/* Main struct for grpc SPIFFE server credential. */
typedef struct grpc_tls_spiffe_server_credentials {
  grpc_server_credentials base;
  grpc_tls_credentials_options* options;
} grpc_tls_spiffe_server_credentials;

/**
 * This method creates a TLS SPIFFE channel credential object.
 *
 * - options: grpc TLS credentials options instance.
 *
 * It returns the created credential object.
 */

grpc_channel_credentials* grpc_tls_spiffe_credentials_create(
    const grpc_tls_credentials_options* options);

/**
 * This method creates a TLS server credential object.
 *
 * - options: grpc TLS credentials options instance.
 *
 * It returns the created credential object.
 */
grpc_server_credentials* grpc_tls_spiffe_server_credentials_create(
    const grpc_tls_credentials_options* options);

#endif /* GRPC_CORE_LIB_SECURITY_CREDENTIALS_TLS_SPIFFE_CREDENTIALS_H */
