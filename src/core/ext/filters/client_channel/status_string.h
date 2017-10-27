/*
 *
 * Copyright 2017 gRPC authors.
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

#ifndef GRPC_CORE_EXT_FILTERS_CLIENT_CHANNEL_STATUS_STRING_H
#define GRPC_CORE_EXT_FILTERS_CLIENT_CHANNEL_STATUS_STRING_H

#include <grpc/status.h>

#include <stdbool.h>

/// If \a status_str is a valid status string, sets \a status to the
/// corresponding status value and returns true.
bool grpc_status_from_string(const char* status_str, grpc_status_code* status);

/// Returns the string form of \a status, or "UNKNWON" if invalid.
const char* grpc_status_string(grpc_status_code status);

#endif /* GRPC_CORE_EXT_FILTERS_CLIENT_CHANNEL_STATUS_STRING_H */
