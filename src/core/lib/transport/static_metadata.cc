/*
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
 */

/*
 * WARNING: Auto-generated code.
 *
 * To make changes to this file, change
 * tools/codegen/core/gen_static_metadata.py, and then re-run it.
 *
 * See metadata.h for an explanation of the interface here, and metadata.cc for
 * an explanation of what's going on.
 */

#include "src/core/lib/transport/static_metadata.h"

#include "src/core/lib/slice/slice_internal.h"

static uint8_t g_bytes[] = {
    58,  112, 97,  116, 104, 58,  109, 101, 116, 104, 111, 100, 58,  115, 116,
    97,  116, 117, 115, 58,  97,  117, 116, 104, 111, 114, 105, 116, 121, 58,
    115, 99,  104, 101, 109, 101, 116, 101, 103, 114, 112, 99,  45,  109, 101,
    115, 115, 97,  103, 101, 103, 114, 112, 99,  45,  115, 116, 97,  116, 117,
    115, 103, 114, 112, 99,  45,  112, 97,  121, 108, 111, 97,  100, 45,  98,
    105, 110, 103, 114, 112, 99,  45,  101, 110, 99,  111, 100, 105, 110, 103,
    103, 114, 112, 99,  45,  97,  99,  99,  101, 112, 116, 45,  101, 110, 99,
    111, 100, 105, 110, 103, 103, 114, 112, 99,  45,  115, 101, 114, 118, 101,
    114, 45,  115, 116, 97,  116, 115, 45,  98,  105, 110, 103, 114, 112, 99,
    45,  116, 97,  103, 115, 45,  98,  105, 110, 103, 114, 112, 99,  45,  116,
    114, 97,  99,  101, 45,  98,  105, 110, 99,  111, 110, 116, 101, 110, 116,
    45,  116, 121, 112, 101, 99,  111, 110, 116, 101, 110, 116, 45,  101, 110,
    99,  111, 100, 105, 110, 103, 97,  99,  99,  101, 112, 116, 45,  101, 110,
    99,  111, 100, 105, 110, 103, 103, 114, 112, 99,  45,  105, 110, 116, 101,
    114, 110, 97,  108, 45,  101, 110, 99,  111, 100, 105, 110, 103, 45,  114,
    101, 113, 117, 101, 115, 116, 103, 114, 112, 99,  45,  105, 110, 116, 101,
    114, 110, 97,  108, 45,  115, 116, 114, 101, 97,  109, 45,  101, 110, 99,
    111, 100, 105, 110, 103, 45,  114, 101, 113, 117, 101, 115, 116, 117, 115,
    101, 114, 45,  97,  103, 101, 110, 116, 104, 111, 115, 116, 108, 98,  45,
    116, 111, 107, 101, 110, 103, 114, 112, 99,  45,  112, 114, 101, 118, 105,
    111, 117, 115, 45,  114, 112, 99,  45,  97,  116, 116, 101, 109, 112, 116,
    115, 103, 114, 112, 99,  45,  114, 101, 116, 114, 121, 45,  112, 117, 115,
    104, 98,  97,  99,  107, 45,  109, 115, 103, 114, 112, 99,  45,  116, 105,
    109, 101, 111, 117, 116, 49,  50,  51,  52,  103, 114, 112, 99,  46,  119,
    97,  105, 116, 95,  102, 111, 114, 95,  114, 101, 97,  100, 121, 103, 114,
    112, 99,  46,  116, 105, 109, 101, 111, 117, 116, 103, 114, 112, 99,  46,
    109, 97,  120, 95,  114, 101, 113, 117, 101, 115, 116, 95,  109, 101, 115,
    115, 97,  103, 101, 95,  98,  121, 116, 101, 115, 103, 114, 112, 99,  46,
    109, 97,  120, 95,  114, 101, 115, 112, 111, 110, 115, 101, 95,  109, 101,
    115, 115, 97,  103, 101, 95,  98,  121, 116, 101, 115, 47,  103, 114, 112,
    99,  46,  108, 98,  46,  118, 49,  46,  76,  111, 97,  100, 66,  97,  108,
    97,  110, 99,  101, 114, 47,  66,  97,  108, 97,  110, 99,  101, 76,  111,
    97,  100, 47,  103, 114, 112, 99,  46,  104, 101, 97,  108, 116, 104, 46,
    118, 49,  46,  72,  101, 97,  108, 116, 104, 47,  87,  97,  116, 99,  104,
    100, 101, 102, 108, 97,  116, 101, 103, 122, 105, 112, 115, 116, 114, 101,
    97,  109, 47,  103, 122, 105, 112, 48,  105, 100, 101, 110, 116, 105, 116,
    121, 116, 114, 97,  105, 108, 101, 114, 115, 97,  112, 112, 108, 105, 99,
    97,  116, 105, 111, 110, 47,  103, 114, 112, 99,  80,  79,  83,  84,  50,
    48,  48,  52,  48,  52,  104, 116, 116, 112, 104, 116, 116, 112, 115, 103,
    114, 112, 99,  71,  69,  84,  80,  85,  84,  47,  47,  105, 110, 100, 101,
    120, 46,  104, 116, 109, 108, 50,  48,  52,  50,  48,  54,  51,  48,  52,
    52,  48,  48,  53,  48,  48,  97,  99,  99,  101, 112, 116, 45,  99,  104,
    97,  114, 115, 101, 116, 103, 122, 105, 112, 44,  32,  100, 101, 102, 108,
    97,  116, 101, 97,  99,  99,  101, 112, 116, 45,  108, 97,  110, 103, 117,
    97,  103, 101, 97,  99,  99,  101, 112, 116, 45,  114, 97,  110, 103, 101,
    115, 97,  99,  99,  101, 112, 116, 97,  99,  99,  101, 115, 115, 45,  99,
    111, 110, 116, 114, 111, 108, 45,  97,  108, 108, 111, 119, 45,  111, 114,
    105, 103, 105, 110, 97,  103, 101, 97,  108, 108, 111, 119, 97,  117, 116,
    104, 111, 114, 105, 122, 97,  116, 105, 111, 110, 99,  97,  99,  104, 101,
    45,  99,  111, 110, 116, 114, 111, 108, 99,  111, 110, 116, 101, 110, 116,
    45,  100, 105, 115, 112, 111, 115, 105, 116, 105, 111, 110, 99,  111, 110,
    116, 101, 110, 116, 45,  108, 97,  110, 103, 117, 97,  103, 101, 99,  111,
    110, 116, 101, 110, 116, 45,  108, 101, 110, 103, 116, 104, 99,  111, 110,
    116, 101, 110, 116, 45,  108, 111, 99,  97,  116, 105, 111, 110, 99,  111,
    110, 116, 101, 110, 116, 45,  114, 97,  110, 103, 101, 99,  111, 111, 107,
    105, 101, 100, 97,  116, 101, 101, 116, 97,  103, 101, 120, 112, 101, 99,
    116, 101, 120, 112, 105, 114, 101, 115, 102, 114, 111, 109, 105, 102, 45,
    109, 97,  116, 99,  104, 105, 102, 45,  109, 111, 100, 105, 102, 105, 101,
    100, 45,  115, 105, 110, 99,  101, 105, 102, 45,  110, 111, 110, 101, 45,
    109, 97,  116, 99,  104, 105, 102, 45,  114, 97,  110, 103, 101, 105, 102,
    45,  117, 110, 109, 111, 100, 105, 102, 105, 101, 100, 45,  115, 105, 110,
    99,  101, 108, 97,  115, 116, 45,  109, 111, 100, 105, 102, 105, 101, 100,
    108, 98,  45,  99,  111, 115, 116, 45,  98,  105, 110, 108, 105, 110, 107,
    108, 111, 99,  97,  116, 105, 111, 110, 109, 97,  120, 45,  102, 111, 114,
    119, 97,  114, 100, 115, 112, 114, 111, 120, 121, 45,  97,  117, 116, 104,
    101, 110, 116, 105, 99,  97,  116, 101, 112, 114, 111, 120, 121, 45,  97,
    117, 116, 104, 111, 114, 105, 122, 97,  116, 105, 111, 110, 114, 97,  110,
    103, 101, 114, 101, 102, 101, 114, 101, 114, 114, 101, 102, 114, 101, 115,
    104, 114, 101, 116, 114, 121, 45,  97,  102, 116, 101, 114, 115, 101, 114,
    118, 101, 114, 115, 101, 116, 45,  99,  111, 111, 107, 105, 101, 115, 116,
    114, 105, 99,  116, 45,  116, 114, 97,  110, 115, 112, 111, 114, 116, 45,
    115, 101, 99,  117, 114, 105, 116, 121, 116, 114, 97,  110, 115, 102, 101,
    114, 45,  101, 110, 99,  111, 100, 105, 110, 103, 118, 97,  114, 121, 118,
    105, 97,  119, 119, 119, 45,  97,  117, 116, 104, 101, 110, 116, 105, 99,
    97,  116, 101, 105, 100, 101, 110, 116, 105, 116, 121, 44,  100, 101, 102,
    108, 97,  116, 101, 105, 100, 101, 110, 116, 105, 116, 121, 44,  103, 122,
    105, 112, 100, 101, 102, 108, 97,  116, 101, 44,  103, 122, 105, 112, 105,
    100, 101, 110, 116, 105, 116, 121, 44,  100, 101, 102, 108, 97,  116, 101,
    44,  103, 122, 105, 112};

static void static_ref(void* unused) {}
static void static_unref(void* unused) {}
static const grpc_slice_refcount_vtable static_sub_vtable = {
    static_ref, static_unref, grpc_slice_default_eq_impl,
    grpc_slice_default_hash_impl};
const grpc_slice_refcount_vtable grpc_static_metadata_vtable = {
    static_ref, static_unref, grpc_static_slice_eq, grpc_static_slice_hash};
static grpc_slice_refcount static_sub_refcnt = {&static_sub_vtable,
                                                &static_sub_refcnt};
grpc_slice_refcount grpc_static_metadata_refcounts[GRPC_STATIC_MDSTR_COUNT] = {
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
    {&grpc_static_metadata_vtable, &static_sub_refcnt},
};

const grpc_slice grpc_static_slice_table[GRPC_STATIC_MDSTR_COUNT] = {
    {&grpc_static_metadata_refcounts[0], {{g_bytes + 0, 5}}},
    {&grpc_static_metadata_refcounts[1], {{g_bytes + 5, 7}}},
    {&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
    {&grpc_static_metadata_refcounts[3], {{g_bytes + 19, 10}}},
    {&grpc_static_metadata_refcounts[4], {{g_bytes + 29, 7}}},
    {&grpc_static_metadata_refcounts[5], {{g_bytes + 36, 2}}},
    {&grpc_static_metadata_refcounts[6], {{g_bytes + 38, 12}}},
    {&grpc_static_metadata_refcounts[7], {{g_bytes + 50, 11}}},
    {&grpc_static_metadata_refcounts[8], {{g_bytes + 61, 16}}},
    {&grpc_static_metadata_refcounts[9], {{g_bytes + 77, 13}}},
    {&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
    {&grpc_static_metadata_refcounts[11], {{g_bytes + 110, 21}}},
    {&grpc_static_metadata_refcounts[12], {{g_bytes + 131, 13}}},
    {&grpc_static_metadata_refcounts[13], {{g_bytes + 144, 14}}},
    {&grpc_static_metadata_refcounts[14], {{g_bytes + 158, 12}}},
    {&grpc_static_metadata_refcounts[15], {{g_bytes + 170, 16}}},
    {&grpc_static_metadata_refcounts[16], {{g_bytes + 186, 15}}},
    {&grpc_static_metadata_refcounts[17], {{g_bytes + 201, 30}}},
    {&grpc_static_metadata_refcounts[18], {{g_bytes + 231, 37}}},
    {&grpc_static_metadata_refcounts[19], {{g_bytes + 268, 10}}},
    {&grpc_static_metadata_refcounts[20], {{g_bytes + 278, 4}}},
    {&grpc_static_metadata_refcounts[21], {{g_bytes + 282, 8}}},
    {&grpc_static_metadata_refcounts[22], {{g_bytes + 290, 26}}},
    {&grpc_static_metadata_refcounts[23], {{g_bytes + 316, 22}}},
    {&grpc_static_metadata_refcounts[24], {{g_bytes + 338, 12}}},
    {&grpc_static_metadata_refcounts[25], {{g_bytes + 350, 1}}},
    {&grpc_static_metadata_refcounts[26], {{g_bytes + 351, 1}}},
    {&grpc_static_metadata_refcounts[27], {{g_bytes + 352, 1}}},
    {&grpc_static_metadata_refcounts[28], {{g_bytes + 353, 1}}},
    {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}},
    {&grpc_static_metadata_refcounts[30], {{g_bytes + 354, 19}}},
    {&grpc_static_metadata_refcounts[31], {{g_bytes + 373, 12}}},
    {&grpc_static_metadata_refcounts[32], {{g_bytes + 385, 30}}},
    {&grpc_static_metadata_refcounts[33], {{g_bytes + 415, 31}}},
    {&grpc_static_metadata_refcounts[34], {{g_bytes + 446, 36}}},
    {&grpc_static_metadata_refcounts[35], {{g_bytes + 482, 28}}},
    {&grpc_static_metadata_refcounts[36], {{g_bytes + 510, 7}}},
    {&grpc_static_metadata_refcounts[37], {{g_bytes + 517, 4}}},
    {&grpc_static_metadata_refcounts[38], {{g_bytes + 521, 11}}},
    {&grpc_static_metadata_refcounts[39], {{g_bytes + 532, 1}}},
    {&grpc_static_metadata_refcounts[40], {{g_bytes + 533, 8}}},
    {&grpc_static_metadata_refcounts[41], {{g_bytes + 541, 8}}},
    {&grpc_static_metadata_refcounts[42], {{g_bytes + 549, 16}}},
    {&grpc_static_metadata_refcounts[43], {{g_bytes + 565, 4}}},
    {&grpc_static_metadata_refcounts[44], {{g_bytes + 569, 3}}},
    {&grpc_static_metadata_refcounts[45], {{g_bytes + 572, 3}}},
    {&grpc_static_metadata_refcounts[46], {{g_bytes + 575, 4}}},
    {&grpc_static_metadata_refcounts[47], {{g_bytes + 579, 5}}},
    {&grpc_static_metadata_refcounts[48], {{g_bytes + 584, 4}}},
    {&grpc_static_metadata_refcounts[49], {{g_bytes + 588, 3}}},
    {&grpc_static_metadata_refcounts[50], {{g_bytes + 591, 3}}},
    {&grpc_static_metadata_refcounts[51], {{g_bytes + 594, 1}}},
    {&grpc_static_metadata_refcounts[52], {{g_bytes + 595, 11}}},
    {&grpc_static_metadata_refcounts[53], {{g_bytes + 606, 3}}},
    {&grpc_static_metadata_refcounts[54], {{g_bytes + 609, 3}}},
    {&grpc_static_metadata_refcounts[55], {{g_bytes + 612, 3}}},
    {&grpc_static_metadata_refcounts[56], {{g_bytes + 615, 3}}},
    {&grpc_static_metadata_refcounts[57], {{g_bytes + 618, 3}}},
    {&grpc_static_metadata_refcounts[58], {{g_bytes + 621, 14}}},
    {&grpc_static_metadata_refcounts[59], {{g_bytes + 635, 13}}},
    {&grpc_static_metadata_refcounts[60], {{g_bytes + 648, 15}}},
    {&grpc_static_metadata_refcounts[61], {{g_bytes + 663, 13}}},
    {&grpc_static_metadata_refcounts[62], {{g_bytes + 676, 6}}},
    {&grpc_static_metadata_refcounts[63], {{g_bytes + 682, 27}}},
    {&grpc_static_metadata_refcounts[64], {{g_bytes + 709, 3}}},
    {&grpc_static_metadata_refcounts[65], {{g_bytes + 712, 5}}},
    {&grpc_static_metadata_refcounts[66], {{g_bytes + 717, 13}}},
    {&grpc_static_metadata_refcounts[67], {{g_bytes + 730, 13}}},
    {&grpc_static_metadata_refcounts[68], {{g_bytes + 743, 19}}},
    {&grpc_static_metadata_refcounts[69], {{g_bytes + 762, 16}}},
    {&grpc_static_metadata_refcounts[70], {{g_bytes + 778, 14}}},
    {&grpc_static_metadata_refcounts[71], {{g_bytes + 792, 16}}},
    {&grpc_static_metadata_refcounts[72], {{g_bytes + 808, 13}}},
    {&grpc_static_metadata_refcounts[73], {{g_bytes + 821, 6}}},
    {&grpc_static_metadata_refcounts[74], {{g_bytes + 827, 4}}},
    {&grpc_static_metadata_refcounts[75], {{g_bytes + 831, 4}}},
    {&grpc_static_metadata_refcounts[76], {{g_bytes + 835, 6}}},
    {&grpc_static_metadata_refcounts[77], {{g_bytes + 841, 7}}},
    {&grpc_static_metadata_refcounts[78], {{g_bytes + 848, 4}}},
    {&grpc_static_metadata_refcounts[79], {{g_bytes + 852, 8}}},
    {&grpc_static_metadata_refcounts[80], {{g_bytes + 860, 17}}},
    {&grpc_static_metadata_refcounts[81], {{g_bytes + 877, 13}}},
    {&grpc_static_metadata_refcounts[82], {{g_bytes + 890, 8}}},
    {&grpc_static_metadata_refcounts[83], {{g_bytes + 898, 19}}},
    {&grpc_static_metadata_refcounts[84], {{g_bytes + 917, 13}}},
    {&grpc_static_metadata_refcounts[85], {{g_bytes + 930, 11}}},
    {&grpc_static_metadata_refcounts[86], {{g_bytes + 941, 4}}},
    {&grpc_static_metadata_refcounts[87], {{g_bytes + 945, 8}}},
    {&grpc_static_metadata_refcounts[88], {{g_bytes + 953, 12}}},
    {&grpc_static_metadata_refcounts[89], {{g_bytes + 965, 18}}},
    {&grpc_static_metadata_refcounts[90], {{g_bytes + 983, 19}}},
    {&grpc_static_metadata_refcounts[91], {{g_bytes + 1002, 5}}},
    {&grpc_static_metadata_refcounts[92], {{g_bytes + 1007, 7}}},
    {&grpc_static_metadata_refcounts[93], {{g_bytes + 1014, 7}}},
    {&grpc_static_metadata_refcounts[94], {{g_bytes + 1021, 11}}},
    {&grpc_static_metadata_refcounts[95], {{g_bytes + 1032, 6}}},
    {&grpc_static_metadata_refcounts[96], {{g_bytes + 1038, 10}}},
    {&grpc_static_metadata_refcounts[97], {{g_bytes + 1048, 25}}},
    {&grpc_static_metadata_refcounts[98], {{g_bytes + 1073, 17}}},
    {&grpc_static_metadata_refcounts[99], {{g_bytes + 1090, 4}}},
    {&grpc_static_metadata_refcounts[100], {{g_bytes + 1094, 3}}},
    {&grpc_static_metadata_refcounts[101], {{g_bytes + 1097, 16}}},
    {&grpc_static_metadata_refcounts[102], {{g_bytes + 1113, 16}}},
    {&grpc_static_metadata_refcounts[103], {{g_bytes + 1129, 13}}},
    {&grpc_static_metadata_refcounts[104], {{g_bytes + 1142, 12}}},
    {&grpc_static_metadata_refcounts[105], {{g_bytes + 1154, 21}}},
};

uintptr_t grpc_static_mdelem_user_data[GRPC_STATIC_MDELEM_COUNT] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 4, 4, 6, 6, 8, 8, 2, 4, 4};

static const int8_t elems_r[] = {
    16, 11, -1, 0,  15,  3,  -65, 12,  0,   18, -5, 0,  0,  0,  18, 7,  -8, 0,
    0,  27, 11, 10, 0,   0,  0,   0,   0,   0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,   0,  0,   0,   0,   0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  -63, 0,  -45, -67, -46, 0,  34, 33, 32, 32, 31, 30, 29, 28,
    28, 27, 26, 25, 24,  23, 22,  21,  20,  20, 19, 19, 18, 17, 16, 15, 14, 13,
    12, 11, 14, 13, 12,  11, 10,  9,   9,   8,  7,  6,  5,  0};
static uint32_t elems_phash(uint32_t i) {
  i -= 51;
  uint32_t x = i % 104;
  uint32_t y = i / 104;
  uint32_t h = x;
  if (y < GPR_ARRAY_SIZE(elems_r)) {
    uint32_t delta = (uint32_t)elems_r[y];
    h += delta;
  }
  return h;
}

static const uint16_t elem_keys[] = {
    1096,  1097,  1725,  571,   1100,  265,  266,  267,  268,   269,   1733,
    155,   156,   1736,  781,   1619,  51,   52,   470,  471,   472,   990,
    991,   1627,  1513,  994,   1630,  767,  768,  2149, 2255,  6177,  1755,
    6495,  6707,  6813,  6919,  1526,  7025, 7131, 7237, 7343,  7449,  2043,
    7555,  7661,  7767,  7873,  7979,  8085, 8191, 8297, 8403,  6389,  8509,
    8615,  6601,  8721,  8827,  8933,  9039, 9145, 9251, 9357,  9463,  9569,
    1162,  1163,  1164,  1165,  9675,  9781, 9887, 9993, 10099, 10205, 1799,
    10311, 10417, 10523, 10629, 10735, 0,    0,    0,    0,     0,     347,
    0,     0,     0,     0,     0,     0,    0,    0,    0,     0,     0,
    0,     0,     0,     0,     0,     0,    0,    0,    0,     0,     0,
    0,     0,     256,   257,   149,   0,    0,    0,    0,     0,     0,
    0,     0,     0,     0,     0,     0,    0,    0,    0,     0,     0,
    0,     0,     0,     0,     0};
static const uint8_t elem_idxs[] = {
    77,  79,  25,  6,   76,  19,  20,  21,  22,  23,  84,  15,  16,  83,  0,
    38,  17,  18,  11,  12,  13,  5,   4,   37,  43,  3,   36,  1,   2,   50,
    57,  24,  26,  28,  30,  31,  32,  7,   33,  34,  35,  39,  40,  72,  41,
    42,  44,  45,  46,  47,  48,  49,  51,  27,  52,  53,  29,  54,  55,  56,
    58,  59,  60,  61,  62,  63,  78,  80,  81,  82,  64,  65,  66,  67,  68,
    69,  85,  70,  71,  73,  74,  75,  255, 255, 255, 255, 255, 14,  255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 9,   10,  8};

grpc_mdelem grpc_static_mdelem_for_static_strings(int a, int b) {
  if (a == -1 || b == -1) return GRPC_MDNULL;
  uint32_t k = (uint32_t)(a * 106 + b);
  uint32_t h = elems_phash(k);
  return h < GPR_ARRAY_SIZE(elem_keys) && elem_keys[h] == k &&
                 elem_idxs[h] != 255
             ? GRPC_MAKE_MDELEM(&grpc_static_mdelem_table[elem_idxs[h]],
                                GRPC_MDELEM_STORAGE_STATIC)
             : GRPC_MDNULL;
}

grpc_mdelem_data grpc_static_mdelem_table[GRPC_STATIC_MDELEM_COUNT] = {
    {{&grpc_static_metadata_refcounts[7], {{g_bytes + 50, 11}}},
     {&grpc_static_metadata_refcounts[39], {{g_bytes + 532, 1}}}},
    {{&grpc_static_metadata_refcounts[7], {{g_bytes + 50, 11}}},
     {&grpc_static_metadata_refcounts[25], {{g_bytes + 350, 1}}}},
    {{&grpc_static_metadata_refcounts[7], {{g_bytes + 50, 11}}},
     {&grpc_static_metadata_refcounts[26], {{g_bytes + 351, 1}}}},
    {{&grpc_static_metadata_refcounts[9], {{g_bytes + 77, 13}}},
     {&grpc_static_metadata_refcounts[40], {{g_bytes + 533, 8}}}},
    {{&grpc_static_metadata_refcounts[9], {{g_bytes + 77, 13}}},
     {&grpc_static_metadata_refcounts[37], {{g_bytes + 517, 4}}}},
    {{&grpc_static_metadata_refcounts[9], {{g_bytes + 77, 13}}},
     {&grpc_static_metadata_refcounts[36], {{g_bytes + 510, 7}}}},
    {{&grpc_static_metadata_refcounts[5], {{g_bytes + 36, 2}}},
     {&grpc_static_metadata_refcounts[41], {{g_bytes + 541, 8}}}},
    {{&grpc_static_metadata_refcounts[14], {{g_bytes + 158, 12}}},
     {&grpc_static_metadata_refcounts[42], {{g_bytes + 549, 16}}}},
    {{&grpc_static_metadata_refcounts[1], {{g_bytes + 5, 7}}},
     {&grpc_static_metadata_refcounts[43], {{g_bytes + 565, 4}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[44], {{g_bytes + 569, 3}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[45], {{g_bytes + 572, 3}}}},
    {{&grpc_static_metadata_refcounts[4], {{g_bytes + 29, 7}}},
     {&grpc_static_metadata_refcounts[46], {{g_bytes + 575, 4}}}},
    {{&grpc_static_metadata_refcounts[4], {{g_bytes + 29, 7}}},
     {&grpc_static_metadata_refcounts[47], {{g_bytes + 579, 5}}}},
    {{&grpc_static_metadata_refcounts[4], {{g_bytes + 29, 7}}},
     {&grpc_static_metadata_refcounts[48], {{g_bytes + 584, 4}}}},
    {{&grpc_static_metadata_refcounts[3], {{g_bytes + 19, 10}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[1], {{g_bytes + 5, 7}}},
     {&grpc_static_metadata_refcounts[49], {{g_bytes + 588, 3}}}},
    {{&grpc_static_metadata_refcounts[1], {{g_bytes + 5, 7}}},
     {&grpc_static_metadata_refcounts[50], {{g_bytes + 591, 3}}}},
    {{&grpc_static_metadata_refcounts[0], {{g_bytes + 0, 5}}},
     {&grpc_static_metadata_refcounts[51], {{g_bytes + 594, 1}}}},
    {{&grpc_static_metadata_refcounts[0], {{g_bytes + 0, 5}}},
     {&grpc_static_metadata_refcounts[52], {{g_bytes + 595, 11}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[53], {{g_bytes + 606, 3}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[54], {{g_bytes + 609, 3}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[55], {{g_bytes + 612, 3}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[56], {{g_bytes + 615, 3}}}},
    {{&grpc_static_metadata_refcounts[2], {{g_bytes + 12, 7}}},
     {&grpc_static_metadata_refcounts[57], {{g_bytes + 618, 3}}}},
    {{&grpc_static_metadata_refcounts[58], {{g_bytes + 621, 14}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[16], {{g_bytes + 186, 15}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[16], {{g_bytes + 186, 15}}},
     {&grpc_static_metadata_refcounts[59], {{g_bytes + 635, 13}}}},
    {{&grpc_static_metadata_refcounts[60], {{g_bytes + 648, 15}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[61], {{g_bytes + 663, 13}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[62], {{g_bytes + 676, 6}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[63], {{g_bytes + 682, 27}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[64], {{g_bytes + 709, 3}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[65], {{g_bytes + 712, 5}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[66], {{g_bytes + 717, 13}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[67], {{g_bytes + 730, 13}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[68], {{g_bytes + 743, 19}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[15], {{g_bytes + 170, 16}}},
     {&grpc_static_metadata_refcounts[40], {{g_bytes + 533, 8}}}},
    {{&grpc_static_metadata_refcounts[15], {{g_bytes + 170, 16}}},
     {&grpc_static_metadata_refcounts[37], {{g_bytes + 517, 4}}}},
    {{&grpc_static_metadata_refcounts[15], {{g_bytes + 170, 16}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[69], {{g_bytes + 762, 16}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[70], {{g_bytes + 778, 14}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[71], {{g_bytes + 792, 16}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[72], {{g_bytes + 808, 13}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[14], {{g_bytes + 158, 12}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[73], {{g_bytes + 821, 6}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[74], {{g_bytes + 827, 4}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[75], {{g_bytes + 831, 4}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[76], {{g_bytes + 835, 6}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[77], {{g_bytes + 841, 7}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[78], {{g_bytes + 848, 4}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[20], {{g_bytes + 278, 4}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[79], {{g_bytes + 852, 8}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[80], {{g_bytes + 860, 17}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[81], {{g_bytes + 877, 13}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[82], {{g_bytes + 890, 8}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[83], {{g_bytes + 898, 19}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[84], {{g_bytes + 917, 13}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[21], {{g_bytes + 282, 8}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[85], {{g_bytes + 930, 11}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[86], {{g_bytes + 941, 4}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[87], {{g_bytes + 945, 8}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[88], {{g_bytes + 953, 12}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[89], {{g_bytes + 965, 18}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[90], {{g_bytes + 983, 19}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[91], {{g_bytes + 1002, 5}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[92], {{g_bytes + 1007, 7}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[93], {{g_bytes + 1014, 7}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[94], {{g_bytes + 1021, 11}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[95], {{g_bytes + 1032, 6}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[96], {{g_bytes + 1038, 10}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[97], {{g_bytes + 1048, 25}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[98], {{g_bytes + 1073, 17}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[19], {{g_bytes + 268, 10}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[99], {{g_bytes + 1090, 4}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[100], {{g_bytes + 1094, 3}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[101], {{g_bytes + 1097, 16}}},
     {&grpc_static_metadata_refcounts[29], {{g_bytes + 354, 0}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[40], {{g_bytes + 533, 8}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[36], {{g_bytes + 510, 7}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[102], {{g_bytes + 1113, 16}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[37], {{g_bytes + 517, 4}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[103], {{g_bytes + 1129, 13}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[104], {{g_bytes + 1142, 12}}}},
    {{&grpc_static_metadata_refcounts[10], {{g_bytes + 90, 20}}},
     {&grpc_static_metadata_refcounts[105], {{g_bytes + 1154, 21}}}},
    {{&grpc_static_metadata_refcounts[16], {{g_bytes + 186, 15}}},
     {&grpc_static_metadata_refcounts[40], {{g_bytes + 533, 8}}}},
    {{&grpc_static_metadata_refcounts[16], {{g_bytes + 186, 15}}},
     {&grpc_static_metadata_refcounts[37], {{g_bytes + 517, 4}}}},
    {{&grpc_static_metadata_refcounts[16], {{g_bytes + 186, 15}}},
     {&grpc_static_metadata_refcounts[103], {{g_bytes + 1129, 13}}}},
};
const uint8_t grpc_static_accept_encoding_metadata[8] = {0,  76, 77, 78,
                                                         79, 80, 81, 82};

const uint8_t grpc_static_accept_stream_encoding_metadata[4] = {0, 83, 84, 85};
