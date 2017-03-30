/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <string.h>

#include <grpc/support/alloc.h>
#include <grpc/support/log.h>

#include "src/core/lib/json/json.h"

grpc_json* grpc_json_create(grpc_json_type type) {
  grpc_json* json = gpr_zalloc(sizeof(*json));
  json->type = type;

  return json;
}

void grpc_json_destroy(grpc_json* json) {
  while (json->child) {
    grpc_json_destroy(json->child);
  }

  if (json->next) {
    json->next->prev = json->prev;
  }

  if (json->prev) {
    json->prev->next = json->next;
  } else if (json->parent) {
    json->parent->child = json->next;
  }

  if (json->owns_value) {
    gpr_free((void*)json->value);
  }
  gpr_free(json);
}

grpc_json* grpc_json_link_child(grpc_json* parent, grpc_json* child, grpc_json* sibling) {
  // first child case.
  if (parent->child == NULL) {
    GPR_ASSERT(sibling == NULL);
    parent->child = child;
    return child;
  }
  if (sibling == NULL) {
    sibling = parent->child;
  }
  // always find the right most sibling.
  while (sibling->next != NULL) {
    sibling = sibling->next;
  }
  sibling->next = child;
  return child;
}

grpc_json* grpc_json_create_child(grpc_json* sibling, grpc_json* parent,
                                  const char* key, const char* value,
                                  grpc_json_type type, bool owns_value) {
  grpc_json* child = grpc_json_create(type);
  grpc_json_link_child(parent, child, sibling);
  child->owns_value = owns_value;
  child->parent = parent;
  child->value = value;
  child->key = key;
  return child;
}
