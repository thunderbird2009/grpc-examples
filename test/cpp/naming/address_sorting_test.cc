#include <grpc/grpc.h>
#include <grpc/support/alloc.h>
#include <grpc/support/host_port.h>
#include <grpc/support/log.h>
#include <grpc/support/string_util.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>
#include <string.h>

#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <gmock/gmock.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>

#include "test/cpp/util/subprocess.h"
#include "test/cpp/util/test_config.h"

extern "C" {
#include "src/core/ext/filters/client_channel/client_channel.h"
#include "src/core/ext/filters/client_channel/resolver.h"
#include "src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.h"
#include "src/core/ext/filters/client_channel/resolver_registry.h"
#include "src/core/lib/channel/channel_args.h"
#include "src/core/lib/iomgr/combiner.h"
#include "src/core/lib/iomgr/executor.h"
#include "src/core/lib/iomgr/iomgr.h"
#include "src/core/lib/iomgr/resolve_address.h"
#include "src/core/lib/iomgr/sockaddr_utils.h"
#include "src/core/lib/support/env.h"
#include "src/core/lib/support/string.h"
#include "test/core/util/port.h"
#include "test/core/util/test_config.h"
}

#include "third_party/address_sorting/address_sorting.h"

namespace {

struct TestAddress {
  std::string dest_addr;
  int family;
};

grpc_resolved_address TestAddressToGrpcResolvedAddress(TestAddress test_addr) {
  char *host;
  char *port;
  grpc_resolved_address resolved_addr;
  gpr_split_host_port(test_addr.dest_addr.c_str(), &host, &port);
  if (test_addr.family == AF_INET) {
    sockaddr_in in_dest;
    in_dest.sin_port = htons(atoi(port));
    ;
    in_dest.sin_family = AF_INET;
    int zeros[2]{0, 0};
    memcpy(&in_dest.sin_zero, &zeros, 8);
    GPR_ASSERT(inet_pton(AF_INET, host, &in_dest.sin_addr) == 1);
    memcpy(&resolved_addr.addr, &in_dest, sizeof(sockaddr_in));
    resolved_addr.len = sizeof(sockaddr_in);
  } else {
    GPR_ASSERT(test_addr.family == AF_INET6);
    sockaddr_in6 in6_dest;
    in6_dest.sin6_port = htons(atoi(port));
    in6_dest.sin6_family = AF_INET6;
    in6_dest.sin6_flowinfo = 0;
    in6_dest.sin6_scope_id = 0;
    GPR_ASSERT(inet_pton(AF_INET6, host, &in6_dest.sin6_addr) == 1);
    memcpy(&resolved_addr.addr, &in6_dest, sizeof(sockaddr_in6));
    resolved_addr.len = sizeof(sockaddr_in6);
  }
  return resolved_addr;
}

class MockSocketFactory : public address_sorting_socket_factory {
 public:
  MockSocketFactory(bool ipv4_supported, bool ipv6_supported,
                    std::map<std::string, TestAddress> dest_addr_to_src_addr)
      : ipv4_supported_(ipv4_supported),
        ipv6_supported_(ipv6_supported),
        dest_addr_to_src_addr_(dest_addr_to_src_addr),
        fd_to_getsockname_return_vals_(std::map<int, TestAddress>()),
        cur_socket_(0) {}

  int Socket(int domain, int type, int protocol) {
    EXPECT_TRUE(domain == AF_INET || domain == AF_INET6);
    if ((domain == AF_INET && !ipv4_supported_) ||
        (domain == AF_INET6 && !ipv6_supported_)) {
      errno = EAFNOSUPPORT;
      return -1;
    }
    return cur_socket_++;
  }

  int Connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    if ((addr->sa_family == AF_INET && !ipv4_supported_) ||
        (addr->sa_family == AF_INET6 && !ipv6_supported_)) {
      errno = EAFNOSUPPORT;
      return -1;
    }
    char *ip_addr_str;
    grpc_resolved_address resolved_addr;
    memcpy(&resolved_addr.addr, addr, addrlen);
    resolved_addr.len = addrlen;
    grpc_sockaddr_to_string(&ip_addr_str, &resolved_addr,
                            false /* normalize */);
    auto it = dest_addr_to_src_addr_.find(ip_addr_str);
    if (it == dest_addr_to_src_addr_.end()) {
      gpr_log(GPR_DEBUG, "can't find |%s| in dest to src map", ip_addr_str);
      errno = ENETUNREACH;
      return -1;
    }
    fd_to_getsockname_return_vals_.insert(
        std::pair<int, TestAddress>(sockfd, it->second));
    return 0;
  }

  int GetSockName(int sockfd, struct sockaddr *addr, socklen_t *addrlen) {
    auto it = fd_to_getsockname_return_vals_.find(sockfd);
    EXPECT_TRUE(it != fd_to_getsockname_return_vals_.end());
    grpc_resolved_address resolved_addr =
        TestAddressToGrpcResolvedAddress(it->second);
    memcpy(addr, &resolved_addr.addr, resolved_addr.len);
    *addrlen = resolved_addr.len;
    return 0;
  }

  static int Close(int sockfd) { return 0; }

 private:
  // user provided test config
  bool ipv4_supported_;
  bool ipv6_supported_;
  std::map<std::string, TestAddress> dest_addr_to_src_addr_;
  // internal state for mock
  std::map<int, TestAddress> fd_to_getsockname_return_vals_;
  int cur_socket_;
};

int mock_socket_factory_wrapper_socket(address_sorting_socket_factory *factory,
                                       int domain, int type, int protocol) {
  MockSocketFactory *mock = reinterpret_cast<MockSocketFactory *>(factory);
  return mock->Socket(domain, type, protocol);
}

int mock_socket_factory_wrapper_connect(address_sorting_socket_factory *factory,
                                        int sockfd, const struct sockaddr *addr,
                                        socklen_t addrlen) {
  MockSocketFactory *mock = reinterpret_cast<MockSocketFactory *>(factory);
  return mock->Connect(sockfd, addr, addrlen);
}

int mock_socket_factory_getsockname(address_sorting_socket_factory *factory,
                                    int sockfd, struct sockaddr *addr,
                                    socklen_t *addrlen) {
  MockSocketFactory *mock = reinterpret_cast<MockSocketFactory *>(factory);
  return mock->GetSockName(sockfd, addr, addrlen);
}

int mock_socket_factory_close(address_sorting_socket_factory *factory,
                              int sockfd) {
  MockSocketFactory *mock = reinterpret_cast<MockSocketFactory *>(factory);
  return mock->Close(sockfd);
}

void mock_socket_factory_wrapper_destroy(
    address_sorting_socket_factory *factory) {
  MockSocketFactory *mock = reinterpret_cast<MockSocketFactory *>(factory);
  delete mock;
}

const address_sorting_socket_factory_vtable kMockSocketFactoryVtable = {
    mock_socket_factory_wrapper_socket,  mock_socket_factory_wrapper_connect,
    mock_socket_factory_getsockname,     mock_socket_factory_close,
    mock_socket_factory_wrapper_destroy,
};

void OverrideAddressSortingSocketFactory(
    bool ipv4_supported, bool ipv6_supported,
    std::map<std::string, TestAddress> dest_addr_to_src_addr) {
  address_sorting_socket_factory *factory = new MockSocketFactory(
      ipv4_supported, ipv6_supported, dest_addr_to_src_addr);
  factory->vtable = &kMockSocketFactoryVtable;
  address_sorting_override_socket_factory_for_testing(factory);
}

grpc_lb_addresses *BuildLbAddrInputs(std::vector<TestAddress> test_addrs) {
  grpc_lb_addresses *lb_addrs = grpc_lb_addresses_create(0, NULL);
  lb_addrs->addresses = (grpc_lb_address *)gpr_zalloc(sizeof(grpc_lb_address) *
                                                      test_addrs.size());
  lb_addrs->num_addresses = test_addrs.size();
  for (size_t i = 0; i < test_addrs.size(); i++) {
    lb_addrs->addresses[i].address =
        TestAddressToGrpcResolvedAddress(test_addrs[i]);
  }
  return lb_addrs;
}

void VerifyLbAddrOutputs(grpc_lb_addresses *lb_addrs,
                         std::vector<std::string> expected_addrs) {
  EXPECT_EQ(lb_addrs->num_addresses, expected_addrs.size());
  for (size_t i = 0; i < lb_addrs->num_addresses; i++) {
    char *ip_addr_str;
    grpc_sockaddr_to_string(&ip_addr_str, &lb_addrs->addresses[i].address,
                            false /* normalize */);
    EXPECT_EQ(expected_addrs[i], ip_addr_str);
    gpr_free(ip_addr_str);
  }
}

}  // namespace

/* Tests for rule 1 */
TEST(AddressSortingTest, TestDepriotizesUnreachableAddresses) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"1.2.3.4:443", {"4.3.2.1:443", AF_INET}},
      });
  auto *lb_addrs = BuildLbAddrInputs({
      {"1.2.3.4:443", AF_INET}, {"5.6.7.8:443", AF_INET},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "1.2.3.4:443", "5.6.7.8:443",
                                });
}

TEST(AddressSortingTest, TestDepriotizesUnsupportedDomainIpv6) {
  bool ipv4_supported = true;
  bool ipv6_supported = false;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"1.2.3.4:443", {"4.3.2.1:0", AF_INET}},
      });
  auto lb_addrs = BuildLbAddrInputs({
      {"[2607:f8b0:400a:801::1002]:443", AF_INET6}, {"1.2.3.4:443", AF_INET},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs,
                      {
                          "1.2.3.4:443", "[2607:f8b0:400a:801::1002]:443",
                      });
}

TEST(AddressSortingTest, TestDepriotizesUnsupportedDomainIpv4) {
  bool ipv4_supported = false;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"1.2.3.4:443", {"4.3.2.1:0", AF_INET}},
          {"[2607:f8b0:400a:801::1002]:443", {"[fec0::1234]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[2607:f8b0:400a:801::1002]:443", AF_INET6}, {"1.2.3.4:443", AF_INET},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs,
                      {
                          "[2607:f8b0:400a:801::1002]:443", "1.2.3.4:443",
                      });
}

/* Tests for rule 2 */

TEST(AddressSortingTest, TestDepriotizesNonMatchingScope) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[2000:f8b0:400a:801::1002]:443",
           {"[fec0::1000]:0", AF_INET6}},  // global and site-local scope
          {"[fec0::5000]:443",
           {"[fec0::5001]:0", AF_INET6}},  // site-local and site-local scope
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[2000:f8b0:400a:801::1002]:443", AF_INET6},
      {"[fec0::5000]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs,
                      {
                          "[fec0::5000]:443", "[2000:f8b0:400a:801::1002]:443",
                      });
}

/* Tests for rule 5 */

TEST(AddressSortingTest, TestUsesLabelFromDefaultTable) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[2002::5001]:443", {"[2001::5002]:0", AF_INET6}},
          {"[2001::5001]:443",
           {"[2001::5002]:0", AF_INET6}},  // matching labels
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[2002::5001]:443", AF_INET6}, {"[2001::5001]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "[2001::5001]:443", "[2002::5001]:443",
                                });
}

/* Tests for rule 6 */

TEST(AddressSortingTest,
     TestUsesDestinationWithHigherPrecedenceWithAnIpv4Address) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[3ffe::5001]:443", {"[3ffe::5002]:0", AF_INET6}},
          {"1.2.3.4:443", {"5.6.7.8:0", AF_INET}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[3ffe::5001]:443", AF_INET6}, {"1.2.3.4:443", AF_INET},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(
      lb_addrs, {
                    // The AF_INET address should be IPv4-mapped by the sort,
                    // and IPv4-mapped
                    // addresses have higher precedence than 3ffe::/16 by spec.
                    "1.2.3.4:443", "[3ffe::5001]:443",
                });
}

TEST(AddressSortingTest,
     TestUsesDestinationWithHigherPrecedenceWith2000PrefixedAddress) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[2001::1234]:443", {"[2001::5678]:0", AF_INET6}},
          {"[2000::5001]:443", {"[2000::5002]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[2001::1234]:443", AF_INET6}, {"[2000::5001]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(
      lb_addrs, {
                    // The 2000::/16 address should match the ::/0 prefix rule
                    "[2000::5001]:443", "[2001::1234]:443",
                });
}

TEST(AddressSortingTest,
     TestUsesDestinationWithHigherPrecedenceWithLinkAndSiteLocalAddresses) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[fec0::1234]:443", {"[fec0::5678]:0", AF_INET6}},
          {"[fc00::5001]:443", {"[fc00::5002]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[fec0::1234]:443", AF_INET6}, {"[fc00::5001]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "[fc00::5001]:443", "[fec0::1234]:443",
                                });
}

/* Tests for rule 8 */

TEST(AddressSortingTest, TestPrefersSmallerScope) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          // Both of these destinations have the same precedence in default
          // policy
          // table.
          {"[fec0::1234]:443", {"[fec0::5678]:0", AF_INET6}},
          {"[3ffe::5001]:443", {"[3ffe::5002]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[3ffe::5001]:443", AF_INET6}, {"[fec0::1234]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "[fec0::1234]:443", "[3ffe::5001]:443",
                                });
}

/* Tests for rule 9 */

TEST(AddressSortingTest, TestPrefersLongestMatchingSrcDstPrefix) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          // Both of these destinations have the same precedence in default
          // policy
          // table.
          {"[3ffe::1234]:443", {"[3ffe::1235]:0", AF_INET6}},
          {"[3ffe::5001]:443", {"[3ffe::4321]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[3ffe::5001]:443", AF_INET6}, {"[3ffe::1234]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "[3ffe::1234]:443", "[3ffe::5001]:443",
                                });
}

/* Tests for rule 10 */

TEST(AddressSortingTest, TestStableSort) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[3ffe::1234]:443", {"[3ffe::1236]:0", AF_INET6}},
          {"[3ffe::1235]:443", {"[3ffe::1237]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[3ffe::1234]:443", AF_INET6}, {"[3ffe::1235]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "[3ffe::1234]:443", "[3ffe::1235]:443",
                                });
}

TEST(AddressSortingTest, TestStableSortFiveElements) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(
      ipv4_supported, ipv6_supported,
      {
          {"[3ffe::1231]:443", {"[3ffe::1201]:0", AF_INET6}},
          {"[3ffe::1232]:443", {"[3ffe::1202]:0", AF_INET6}},
          {"[3ffe::1233]:443", {"[3ffe::1203]:0", AF_INET6}},
          {"[3ffe::1234]:443", {"[3ffe::1204]:0", AF_INET6}},
          {"[3ffe::1235]:443", {"[3ffe::1205]:0", AF_INET6}},
      });
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[3ffe::1231]:443", AF_INET6},
      {"[3ffe::1232]:443", AF_INET6},
      {"[3ffe::1233]:443", AF_INET6},
      {"[3ffe::1234]:443", AF_INET6},
      {"[3ffe::1235]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(
      lb_addrs, {
                    "[3ffe::1231]:443", "[3ffe::1232]:443", "[3ffe::1233]:443",
                    "[3ffe::1234]:443", "[3ffe::1235]:443",
                });
}

TEST(AddressSortingTest, TestStableSortNoSrcAddrsExist) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(ipv4_supported, ipv6_supported, {});
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[3ffe::1231]:443", AF_INET6},
      {"[3ffe::1232]:443", AF_INET6},
      {"[3ffe::1233]:443", AF_INET6},
      {"[3ffe::1234]:443", AF_INET6},
      {"[3ffe::1235]:443", AF_INET6},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(
      lb_addrs, {
                    "[3ffe::1231]:443", "[3ffe::1232]:443", "[3ffe::1233]:443",
                    "[3ffe::1234]:443", "[3ffe::1235]:443",
                });
}

TEST(AddressSortingTest, TestStableSortNoSrcAddrsExistWithIpv4) {
  bool ipv4_supported = true;
  bool ipv6_supported = true;
  OverrideAddressSortingSocketFactory(ipv4_supported, ipv6_supported, {});
  grpc_lb_addresses *lb_addrs = BuildLbAddrInputs({
      {"[::ffff:5.6.7.8]:443", AF_INET6}, {"1.2.3.4:443", AF_INET},
  });
  address_sorting_rfc_6724_sort(lb_addrs);
  VerifyLbAddrOutputs(lb_addrs, {
                                    "[::ffff:5.6.7.8]:443", "1.2.3.4:443",
                                });
}

int main(int argc, char **argv) {
  const char* resolver = gpr_getenv("GRPC_DNS_RESOLVER");
  if (resolver == NULL || strlen(resolver) == 0) {
    gpr_setenv("GRPC_DNS_RESOLVER", "ares");
  } else if (strcmp("ares", resolver)) {
    gpr_log(GPR_INFO, "GRPC_DNS_RESOLVER != ares: %s.", resolver);
  }
  grpc_test_init(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  grpc_init();
  auto result = RUN_ALL_TESTS();
  grpc_shutdown();
  return result;
}