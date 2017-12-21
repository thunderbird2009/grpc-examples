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

#ifndef GRPC_CORE_LIB_SUPPORT_ORPHANABLE_H
#define GRPC_CORE_LIB_SUPPORT_ORPHANABLE_H

#include <grpc/support/log.h>
#include <grpc/support/sync.h>

#include <memory>

#include "src/core/lib/debug/trace.h"
#include "src/core/lib/support/abstract.h"
#include "src/core/lib/support/debug_location.h"
#include "src/core/lib/support/memory.h"

namespace grpc_core {

// A base class for orphanable objects.
class Orphanable {
 public:
  // Gives up ownership of the object.  The implementation must arrange
  // to destroy the object without further interaction from the caller.
  virtual void Orphan() GRPC_ABSTRACT;

  // Not copyable or movable.
  Orphanable(const Orphanable&) = delete;
  Orphanable& operator=(const Orphanable&) = delete;

  GRPC_ABSTRACT_BASE_CLASS

 protected:
  Orphanable() {}
  virtual ~Orphanable() {}
};

template <typename T>
class OrphanableDelete {
 public:
  void operator()(T* p) { p->Orphan(); }
};

template <typename T, typename Deleter = OrphanableDelete<T>>
using OrphanablePtr = std::unique_ptr<T, Deleter>;

template <typename T, typename... Args>
inline OrphanablePtr<T> MakeOrphanable(Args&&... args) {
  return OrphanablePtr<T>(New<T>(std::forward<Args>(args)...));
}

// A type of Orphanable with internal ref-counting.
class InternallyRefCounted : public Orphanable {
 public:
  // Not copyable nor movable.
  InternallyRefCounted(const InternallyRefCounted&) = delete;
  InternallyRefCounted& operator=(const InternallyRefCounted&) = delete;

  GRPC_ABSTRACT_BASE_CLASS

 protected:
  InternallyRefCounted() { gpr_ref_init(&refs_, 1); }
  virtual ~InternallyRefCounted() {}

  void Ref() { gpr_ref(&refs_); }

  void Unref() {
    if (gpr_unref(&refs_)) {
      Delete(this);
    }
  }

  // Allow Delete() to access destructor.
  template <typename T>
  friend void Delete(T*);

 private:
  gpr_refcount refs_;
};

// An alternative version of the InternallyRefCounted base class that
// supports tracing.  This is intended to be used in cases where the
// object will be handled both by idiomatic C++ code using smart
// pointers and legacy code that is manually calling Ref() and Unref().
// Once all of our code is converted to idiomatic C++, we may be able to
// eliminate this class.
class InternallyRefCountedWithTracing : public Orphanable {
 public:
  // Not copyable nor movable.
  InternallyRefCountedWithTracing(const InternallyRefCountedWithTracing&) =
      delete;
  InternallyRefCountedWithTracing& operator=(
      const InternallyRefCountedWithTracing&) = delete;

  GRPC_ABSTRACT_BASE_CLASS

 protected:
  // Allow Delete() to access destructor.
  template <typename T>
  friend void Delete(T*);

  InternallyRefCountedWithTracing()
      : InternallyRefCountedWithTracing(nullptr) {}

  explicit InternallyRefCountedWithTracing(TraceFlag* trace_flag)
      : trace_flag_(trace_flag) {
    gpr_ref_init(&refs_, 1);
  }

  virtual ~InternallyRefCountedWithTracing() {}

  void Ref() { gpr_ref(&refs_); }

  void Ref(const DebugLocation& location, const char* reason) {
    if (location.Log() && trace_flag_ != nullptr && trace_flag_->enabled()) {
      gpr_atm old_refs = gpr_atm_no_barrier_load(&refs_.count);
      gpr_log(GPR_DEBUG, "%s:%p %s:%d ref %" PRIdPTR " -> %" PRIdPTR " %s",
              trace_flag_->name(), this, location.file(), location.line(),
              old_refs, old_refs + 1, reason);
    }
    Ref();
  }

  void Unref() {
    if (gpr_unref(&refs_)) {
      Delete(this);
    }
  }

  void Unref(const DebugLocation& location, const char* reason) {
    if (location.Log() && trace_flag_ != nullptr && trace_flag_->enabled()) {
      gpr_atm old_refs = gpr_atm_no_barrier_load(&refs_.count);
      gpr_log(GPR_DEBUG, "%s:%p %s:%d unref %" PRIdPTR " -> %" PRIdPTR " %s",
              trace_flag_->name(), this, location.file(), location.line(),
              old_refs, old_refs - 1, reason);
    }
    Unref();
  }

 private:
  TraceFlag* trace_flag_ = nullptr;
  gpr_refcount refs_;
};

}  // namespace grpc_core

#endif /* GRPC_CORE_LIB_SUPPORT_ORPHANABLE_H */
