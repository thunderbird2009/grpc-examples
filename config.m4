PHP_ARG_ENABLE(grpc, whether to enable grpc support,
[  --enable-grpc           Enable grpc support])

if test "$PHP_GRPC" != "no"; then
  dnl Write more examples of tests here...

  dnl # --with-grpc -> add include path
  PHP_ADD_INCLUDE(PHP_EXT_SRCDIR()/include)
  PHP_ADD_INCLUDE(PHP_EXT_SRCDIR()/src/php/ext/grpc)
  PHP_ADD_INCLUDE(PHP_EXT_SRCDIR()/third_party/boringssl/include)

  LIBS="-lpthread $LIBS"

  CFLAGS="-Wall -Werror -Wno-parentheses-equality -Wno-unused-value -std=c11"
  CXXFLAGS="-std=c++11 -fno-exceptions -fno-rtti"
  GRPC_SHARED_LIBADD="-lpthread $GRPC_SHARED_LIBADD"
  PHP_REQUIRE_CXX()
  PHP_ADD_LIBRARY(pthread)
  PHP_ADD_LIBRARY(dl,,GRPC_SHARED_LIBADD)
  PHP_ADD_LIBRARY(dl)

  case $host in
    *darwin*)
      ;;
    *)
      PHP_ADD_LIBRARY(rt,,GRPC_SHARED_LIBADD)
      PHP_ADD_LIBRARY(rt)
      ;;
  esac

  PHP_NEW_EXTENSION(grpc,
    src/php/ext/grpc/byte_buffer.c \
    src/php/ext/grpc/call.c \
    src/php/ext/grpc/call_credentials.c \
    src/php/ext/grpc/channel.c \
    src/php/ext/grpc/channel_credentials.c \
    src/php/ext/grpc/completion_queue.c \
    src/php/ext/grpc/php_grpc.c \
    src/php/ext/grpc/server.c \
    src/php/ext/grpc/server_credentials.c \
    src/php/ext/grpc/timeval.c \
    src/core/lib/profiling/basic_timers.cc \
    src/core/lib/profiling/stap_timers.cc \
    src/core/lib/support/alloc.cc \
    src/core/lib/support/arena.cc \
    src/core/lib/support/atm.cc \
    src/core/lib/support/avl.cc \
    src/core/lib/support/cmdline.cc \
    src/core/lib/support/cpu_iphone.cc \
    src/core/lib/support/cpu_linux.cc \
    src/core/lib/support/cpu_posix.cc \
    src/core/lib/support/cpu_windows.cc \
    src/core/lib/support/env_linux.cc \
    src/core/lib/support/env_posix.cc \
    src/core/lib/support/env_windows.cc \
    src/core/lib/support/fork.cc \
    src/core/lib/support/host_port.cc \
    src/core/lib/support/log.cc \
    src/core/lib/support/log_android.cc \
    src/core/lib/support/log_linux.cc \
    src/core/lib/support/log_posix.cc \
    src/core/lib/support/log_windows.cc \
    src/core/lib/support/mpscq.cc \
    src/core/lib/support/murmur_hash.cc \
    src/core/lib/support/string.cc \
    src/core/lib/support/string_posix.cc \
    src/core/lib/support/string_util_windows.cc \
    src/core/lib/support/string_windows.cc \
    src/core/lib/support/subprocess_posix.cc \
    src/core/lib/support/subprocess_windows.cc \
    src/core/lib/support/sync.cc \
    src/core/lib/support/sync_posix.cc \
    src/core/lib/support/sync_windows.cc \
    src/core/lib/support/thd.cc \
    src/core/lib/support/thd_posix.cc \
    src/core/lib/support/thd_windows.cc \
    src/core/lib/support/time.cc \
    src/core/lib/support/time_posix.cc \
    src/core/lib/support/time_precise.cc \
    src/core/lib/support/time_windows.cc \
    src/core/lib/support/tls_pthread.cc \
    src/core/lib/support/tmpfile_msys.cc \
    src/core/lib/support/tmpfile_posix.cc \
    src/core/lib/support/tmpfile_windows.cc \
    src/core/lib/support/wrap_memcpy.cc \
    src/core/lib/surface/init.cc \
    src/core/lib/backoff/backoff.cc \
    src/core/lib/channel/channel_args.cc \
    src/core/lib/channel/channel_stack.cc \
    src/core/lib/channel/channel_stack_builder.cc \
    src/core/lib/channel/connected_channel.cc \
    src/core/lib/channel/handshaker.cc \
    src/core/lib/channel/handshaker_factory.cc \
    src/core/lib/channel/handshaker_registry.cc \
    src/core/lib/compression/compression.cc \
    src/core/lib/compression/message_compress.cc \
    src/core/lib/compression/stream_compression.cc \
    src/core/lib/compression/stream_compression_gzip.cc \
    src/core/lib/compression/stream_compression_identity.cc \
    src/core/lib/debug/stats.cc \
    src/core/lib/debug/stats_data.cc \
    src/core/lib/http/format_request.cc \
    src/core/lib/http/httpcli.cc \
    src/core/lib/http/parser.cc \
    src/core/lib/iomgr/call_combiner.cc \
    src/core/lib/iomgr/combiner.cc \
    src/core/lib/iomgr/endpoint.cc \
    src/core/lib/iomgr/endpoint_pair_posix.cc \
    src/core/lib/iomgr/endpoint_pair_uv.cc \
    src/core/lib/iomgr/endpoint_pair_windows.cc \
    src/core/lib/iomgr/error.cc \
    src/core/lib/iomgr/ev_epoll1_linux.cc \
    src/core/lib/iomgr/ev_epollex_linux.cc \
    src/core/lib/iomgr/ev_epollsig_linux.cc \
    src/core/lib/iomgr/ev_poll_posix.cc \
    src/core/lib/iomgr/ev_posix.cc \
    src/core/lib/iomgr/ev_windows.cc \
    src/core/lib/iomgr/exec_ctx.cc \
    src/core/lib/iomgr/executor.cc \
    src/core/lib/iomgr/fork_posix.cc \
    src/core/lib/iomgr/fork_windows.cc \
    src/core/lib/iomgr/gethostname_fallback.cc \
    src/core/lib/iomgr/gethostname_host_name_max.cc \
    src/core/lib/iomgr/gethostname_sysconf.cc \
    src/core/lib/iomgr/iocp_windows.cc \
    src/core/lib/iomgr/iomgr.cc \
    src/core/lib/iomgr/iomgr_posix.cc \
    src/core/lib/iomgr/iomgr_uv.cc \
    src/core/lib/iomgr/iomgr_windows.cc \
    src/core/lib/iomgr/is_epollexclusive_available.cc \
    src/core/lib/iomgr/load_file.cc \
    src/core/lib/iomgr/lockfree_event.cc \
    src/core/lib/iomgr/network_status_tracker.cc \
    src/core/lib/iomgr/polling_entity.cc \
    src/core/lib/iomgr/pollset_set_uv.cc \
    src/core/lib/iomgr/pollset_set_windows.cc \
    src/core/lib/iomgr/pollset_uv.cc \
    src/core/lib/iomgr/pollset_windows.cc \
    src/core/lib/iomgr/resolve_address_posix.cc \
    src/core/lib/iomgr/resolve_address_uv.cc \
    src/core/lib/iomgr/resolve_address_windows.cc \
    src/core/lib/iomgr/resource_quota.cc \
    src/core/lib/iomgr/sockaddr_utils.cc \
    src/core/lib/iomgr/socket_factory_posix.cc \
    src/core/lib/iomgr/socket_mutator.cc \
    src/core/lib/iomgr/socket_utils_common_posix.cc \
    src/core/lib/iomgr/socket_utils_linux.cc \
    src/core/lib/iomgr/socket_utils_posix.cc \
    src/core/lib/iomgr/socket_utils_uv.cc \
    src/core/lib/iomgr/socket_utils_windows.cc \
    src/core/lib/iomgr/socket_windows.cc \
    src/core/lib/iomgr/tcp_client_posix.cc \
    src/core/lib/iomgr/tcp_client_uv.cc \
    src/core/lib/iomgr/tcp_client_windows.cc \
    src/core/lib/iomgr/tcp_posix.cc \
    src/core/lib/iomgr/tcp_server_posix.cc \
    src/core/lib/iomgr/tcp_server_utils_posix_common.cc \
    src/core/lib/iomgr/tcp_server_utils_posix_ifaddrs.cc \
    src/core/lib/iomgr/tcp_server_utils_posix_noifaddrs.cc \
    src/core/lib/iomgr/tcp_server_uv.cc \
    src/core/lib/iomgr/tcp_server_windows.cc \
    src/core/lib/iomgr/tcp_uv.cc \
    src/core/lib/iomgr/tcp_windows.cc \
    src/core/lib/iomgr/time_averaged_stats.cc \
    src/core/lib/iomgr/timer_generic.cc \
    src/core/lib/iomgr/timer_heap.cc \
    src/core/lib/iomgr/timer_manager.cc \
    src/core/lib/iomgr/timer_uv.cc \
    src/core/lib/iomgr/udp_server.cc \
    src/core/lib/iomgr/unix_sockets_posix.cc \
    src/core/lib/iomgr/unix_sockets_posix_noop.cc \
    src/core/lib/iomgr/wakeup_fd_cv.cc \
    src/core/lib/iomgr/wakeup_fd_eventfd.cc \
    src/core/lib/iomgr/wakeup_fd_nospecial.cc \
    src/core/lib/iomgr/wakeup_fd_pipe.cc \
    src/core/lib/iomgr/wakeup_fd_posix.cc \
    src/core/lib/json/json.cc \
    src/core/lib/json/json_reader.cc \
    src/core/lib/json/json_string.cc \
    src/core/lib/json/json_writer.cc \
    src/core/lib/slice/b64.cc \
    src/core/lib/slice/percent_encoding.cc \
    src/core/lib/slice/slice.cc \
    src/core/lib/slice/slice_buffer.cc \
    src/core/lib/slice/slice_hash_table.cc \
    src/core/lib/slice/slice_intern.cc \
    src/core/lib/slice/slice_string_helpers.cc \
    src/core/lib/surface/alarm.cc \
    src/core/lib/surface/api_trace.cc \
    src/core/lib/surface/byte_buffer.cc \
    src/core/lib/surface/byte_buffer_reader.cc \
    src/core/lib/surface/call.cc \
    src/core/lib/surface/call_details.cc \
    src/core/lib/surface/call_log_batch.cc \
    src/core/lib/surface/channel.cc \
    src/core/lib/surface/channel_init.cc \
    src/core/lib/surface/channel_ping.cc \
    src/core/lib/surface/channel_stack_type.cc \
    src/core/lib/surface/completion_queue.cc \
    src/core/lib/surface/completion_queue_factory.cc \
    src/core/lib/surface/event_string.cc \
    src/core/lib/surface/lame_client.cc \
    src/core/lib/surface/metadata_array.cc \
    src/core/lib/surface/server.cc \
    src/core/lib/surface/validate_metadata.cc \
    src/core/lib/surface/version.cc \
    src/core/lib/transport/bdp_estimator.cc \
    src/core/lib/transport/byte_stream.cc \
    src/core/lib/transport/connectivity_state.cc \
    src/core/lib/transport/error_utils.cc \
    src/core/lib/transport/metadata.cc \
    src/core/lib/transport/metadata_batch.cc \
    src/core/lib/transport/pid_controller.cc \
    src/core/lib/transport/service_config.cc \
    src/core/lib/transport/static_metadata.cc \
    src/core/lib/transport/status_conversion.cc \
    src/core/lib/transport/timeout_encoding.cc \
    src/core/lib/transport/transport.cc \
    src/core/lib/transport/transport_op_string.cc \
    src/core/lib/debug/trace.cc \
    src/core/ext/transport/chttp2/server/secure/server_secure_chttp2.cc \
    src/core/ext/transport/chttp2/transport/bin_decoder.cc \
    src/core/ext/transport/chttp2/transport/bin_encoder.cc \
    src/core/ext/transport/chttp2/transport/chttp2_plugin.cc \
    src/core/ext/transport/chttp2/transport/chttp2_transport.cc \
    src/core/ext/transport/chttp2/transport/flow_control.cc \
    src/core/ext/transport/chttp2/transport/frame_data.cc \
    src/core/ext/transport/chttp2/transport/frame_goaway.cc \
    src/core/ext/transport/chttp2/transport/frame_ping.cc \
    src/core/ext/transport/chttp2/transport/frame_rst_stream.cc \
    src/core/ext/transport/chttp2/transport/frame_settings.cc \
    src/core/ext/transport/chttp2/transport/frame_window_update.cc \
    src/core/ext/transport/chttp2/transport/hpack_encoder.cc \
    src/core/ext/transport/chttp2/transport/hpack_parser.cc \
    src/core/ext/transport/chttp2/transport/hpack_table.cc \
    src/core/ext/transport/chttp2/transport/http2_settings.cc \
    src/core/ext/transport/chttp2/transport/huffsyms.cc \
    src/core/ext/transport/chttp2/transport/incoming_metadata.cc \
    src/core/ext/transport/chttp2/transport/parsing.cc \
    src/core/ext/transport/chttp2/transport/stream_lists.cc \
    src/core/ext/transport/chttp2/transport/stream_map.cc \
    src/core/ext/transport/chttp2/transport/varint.cc \
    src/core/ext/transport/chttp2/transport/writing.cc \
    src/core/ext/transport/chttp2/alpn/alpn.cc \
    src/core/ext/filters/http/client/http_client_filter.cc \
    src/core/ext/filters/http/http_filters_plugin.cc \
    src/core/ext/filters/http/message_compress/message_compress_filter.cc \
    src/core/ext/filters/http/server/http_server_filter.cc \
    src/core/lib/http/httpcli_security_connector.cc \
    src/core/lib/security/context/security_context.cc \
    src/core/lib/security/credentials/composite/composite_credentials.cc \
    src/core/lib/security/credentials/credentials.cc \
    src/core/lib/security/credentials/credentials_metadata.cc \
    src/core/lib/security/credentials/fake/fake_credentials.cc \
    src/core/lib/security/credentials/google_default/credentials_generic.cc \
    src/core/lib/security/credentials/google_default/google_default_credentials.cc \
    src/core/lib/security/credentials/iam/iam_credentials.cc \
    src/core/lib/security/credentials/jwt/json_token.cc \
    src/core/lib/security/credentials/jwt/jwt_credentials.cc \
    src/core/lib/security/credentials/jwt/jwt_verifier.cc \
    src/core/lib/security/credentials/oauth2/oauth2_credentials.cc \
    src/core/lib/security/credentials/plugin/plugin_credentials.cc \
    src/core/lib/security/credentials/ssl/ssl_credentials.cc \
    src/core/lib/security/transport/client_auth_filter.cc \
    src/core/lib/security/transport/lb_targets_info.cc \
    src/core/lib/security/transport/secure_endpoint.cc \
    src/core/lib/security/transport/security_connector.cc \
    src/core/lib/security/transport/security_handshaker.cc \
    src/core/lib/security/transport/server_auth_filter.cc \
    src/core/lib/security/transport/tsi_error.cc \
    src/core/lib/security/util/json_util.cc \
    src/core/lib/surface/init_secure.cc \
    src/core/tsi/fake_transport_security.cc \
    src/core/tsi/gts_transport_security.cc \
    src/core/tsi/ssl_transport_security.cc \
    src/core/tsi/transport_security_grpc.cc \
    src/core/tsi/transport_security.cc \
    src/core/tsi/transport_security_adapter.cc \
    src/core/ext/transport/chttp2/server/chttp2_server.cc \
    src/core/ext/transport/chttp2/client/secure/secure_channel_create.cc \
    src/core/ext/filters/client_channel/backup_poller.cc \
    src/core/ext/filters/client_channel/channel_connectivity.cc \
    src/core/ext/filters/client_channel/client_channel.cc \
    src/core/ext/filters/client_channel/client_channel_factory.cc \
    src/core/ext/filters/client_channel/client_channel_plugin.cc \
    src/core/ext/filters/client_channel/connector.cc \
    src/core/ext/filters/client_channel/http_connect_handshaker.cc \
    src/core/ext/filters/client_channel/http_proxy.cc \
    src/core/ext/filters/client_channel/lb_policy.cc \
    src/core/ext/filters/client_channel/lb_policy_factory.cc \
    src/core/ext/filters/client_channel/lb_policy_registry.cc \
    src/core/ext/filters/client_channel/parse_address.cc \
    src/core/ext/filters/client_channel/proxy_mapper.cc \
    src/core/ext/filters/client_channel/proxy_mapper_registry.cc \
    src/core/ext/filters/client_channel/resolver.cc \
    src/core/ext/filters/client_channel/resolver_registry.cc \
    src/core/ext/filters/client_channel/retry_throttle.cc \
    src/core/ext/filters/client_channel/subchannel.cc \
    src/core/ext/filters/client_channel/subchannel_index.cc \
    src/core/ext/filters/client_channel/uri_parser.cc \
    src/core/ext/filters/deadline/deadline_filter.cc \
    src/core/ext/transport/chttp2/client/chttp2_connector.cc \
    src/core/ext/transport/chttp2/server/insecure/server_chttp2.cc \
    src/core/ext/transport/chttp2/server/insecure/server_chttp2_posix.cc \
    src/core/ext/transport/chttp2/client/insecure/channel_create.cc \
    src/core/ext/transport/chttp2/client/insecure/channel_create_posix.cc \
    src/core/ext/transport/inproc/inproc_plugin.cc \
    src/core/ext/transport/inproc/inproc_transport.cc \
    src/core/ext/filters/client_channel/lb_policy/grpclb/client_load_reporting_filter.cc \
    src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb.cc \
    src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_channel_secure.cc \
    src/core/ext/filters/client_channel/lb_policy/grpclb/grpclb_client_stats.cc \
    src/core/ext/filters/client_channel/lb_policy/grpclb/load_balancer_api.cc \
    src/core/ext/filters/client_channel/lb_policy/grpclb/proto/grpc/lb/v1/load_balancer.pb.c \
    third_party/nanopb/pb_common.c \
    third_party/nanopb/pb_decode.c \
    third_party/nanopb/pb_encode.c \
    src/core/ext/filters/client_channel/resolver/fake/fake_resolver.cc \
    src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc \
    src/core/ext/filters/client_channel/lb_policy/subchannel_list.cc \
    src/core/ext/filters/client_channel/lb_policy/round_robin/round_robin.cc \
    src/core/ext/filters/client_channel/resolver/dns/c_ares/dns_resolver_ares.cc \
    src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_ev_driver_posix.cc \
    src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper.cc \
    src/core/ext/filters/client_channel/resolver/dns/c_ares/grpc_ares_wrapper_fallback.cc \
    src/core/ext/filters/client_channel/resolver/dns/native/dns_resolver.cc \
    src/core/ext/filters/client_channel/resolver/sockaddr/sockaddr_resolver.cc \
    src/core/ext/filters/load_reporting/server_load_reporting_filter.cc \
    src/core/ext/filters/load_reporting/server_load_reporting_plugin.cc \
    src/core/ext/census/grpc_context.cc \
    src/core/ext/filters/max_age/max_age_filter.cc \
    src/core/ext/filters/message_size/message_size_filter.cc \
    src/core/ext/filters/workarounds/workaround_cronet_compression_filter.cc \
    src/core/ext/filters/workarounds/workaround_utils.cc \
    src/core/plugin_registry/grpc_plugin_registry.cc \
    src/boringssl/err_data.c \
    third_party/boringssl/crypto/aes/aes.c \
    third_party/boringssl/crypto/aes/key_wrap.c \
    third_party/boringssl/crypto/aes/mode_wrappers.c \
    third_party/boringssl/crypto/asn1/a_bitstr.c \
    third_party/boringssl/crypto/asn1/a_bool.c \
    third_party/boringssl/crypto/asn1/a_d2i_fp.c \
    third_party/boringssl/crypto/asn1/a_dup.c \
    third_party/boringssl/crypto/asn1/a_enum.c \
    third_party/boringssl/crypto/asn1/a_gentm.c \
    third_party/boringssl/crypto/asn1/a_i2d_fp.c \
    third_party/boringssl/crypto/asn1/a_int.c \
    third_party/boringssl/crypto/asn1/a_mbstr.c \
    third_party/boringssl/crypto/asn1/a_object.c \
    third_party/boringssl/crypto/asn1/a_octet.c \
    third_party/boringssl/crypto/asn1/a_print.c \
    third_party/boringssl/crypto/asn1/a_strnid.c \
    third_party/boringssl/crypto/asn1/a_time.c \
    third_party/boringssl/crypto/asn1/a_type.c \
    third_party/boringssl/crypto/asn1/a_utctm.c \
    third_party/boringssl/crypto/asn1/a_utf8.c \
    third_party/boringssl/crypto/asn1/asn1_lib.c \
    third_party/boringssl/crypto/asn1/asn1_par.c \
    third_party/boringssl/crypto/asn1/asn_pack.c \
    third_party/boringssl/crypto/asn1/f_enum.c \
    third_party/boringssl/crypto/asn1/f_int.c \
    third_party/boringssl/crypto/asn1/f_string.c \
    third_party/boringssl/crypto/asn1/t_bitst.c \
    third_party/boringssl/crypto/asn1/tasn_dec.c \
    third_party/boringssl/crypto/asn1/tasn_enc.c \
    third_party/boringssl/crypto/asn1/tasn_fre.c \
    third_party/boringssl/crypto/asn1/tasn_new.c \
    third_party/boringssl/crypto/asn1/tasn_typ.c \
    third_party/boringssl/crypto/asn1/tasn_utl.c \
    third_party/boringssl/crypto/asn1/time_support.c \
    third_party/boringssl/crypto/asn1/x_bignum.c \
    third_party/boringssl/crypto/asn1/x_long.c \
    third_party/boringssl/crypto/base64/base64.c \
    third_party/boringssl/crypto/bio/bio.c \
    third_party/boringssl/crypto/bio/bio_mem.c \
    third_party/boringssl/crypto/bio/connect.c \
    third_party/boringssl/crypto/bio/fd.c \
    third_party/boringssl/crypto/bio/file.c \
    third_party/boringssl/crypto/bio/hexdump.c \
    third_party/boringssl/crypto/bio/pair.c \
    third_party/boringssl/crypto/bio/printf.c \
    third_party/boringssl/crypto/bio/socket.c \
    third_party/boringssl/crypto/bio/socket_helper.c \
    third_party/boringssl/crypto/bn/add.c \
    third_party/boringssl/crypto/bn/asm/x86_64-gcc.c \
    third_party/boringssl/crypto/bn/bn.c \
    third_party/boringssl/crypto/bn/bn_asn1.c \
    third_party/boringssl/crypto/bn/cmp.c \
    third_party/boringssl/crypto/bn/convert.c \
    third_party/boringssl/crypto/bn/ctx.c \
    third_party/boringssl/crypto/bn/div.c \
    third_party/boringssl/crypto/bn/exponentiation.c \
    third_party/boringssl/crypto/bn/gcd.c \
    third_party/boringssl/crypto/bn/generic.c \
    third_party/boringssl/crypto/bn/kronecker.c \
    third_party/boringssl/crypto/bn/montgomery.c \
    third_party/boringssl/crypto/bn/montgomery_inv.c \
    third_party/boringssl/crypto/bn/mul.c \
    third_party/boringssl/crypto/bn/prime.c \
    third_party/boringssl/crypto/bn/random.c \
    third_party/boringssl/crypto/bn/rsaz_exp.c \
    third_party/boringssl/crypto/bn/shift.c \
    third_party/boringssl/crypto/bn/sqrt.c \
    third_party/boringssl/crypto/buf/buf.c \
    third_party/boringssl/crypto/bytestring/asn1_compat.c \
    third_party/boringssl/crypto/bytestring/ber.c \
    third_party/boringssl/crypto/bytestring/cbb.c \
    third_party/boringssl/crypto/bytestring/cbs.c \
    third_party/boringssl/crypto/chacha/chacha.c \
    third_party/boringssl/crypto/cipher/aead.c \
    third_party/boringssl/crypto/cipher/cipher.c \
    third_party/boringssl/crypto/cipher/derive_key.c \
    third_party/boringssl/crypto/cipher/e_aes.c \
    third_party/boringssl/crypto/cipher/e_chacha20poly1305.c \
    third_party/boringssl/crypto/cipher/e_des.c \
    third_party/boringssl/crypto/cipher/e_null.c \
    third_party/boringssl/crypto/cipher/e_rc2.c \
    third_party/boringssl/crypto/cipher/e_rc4.c \
    third_party/boringssl/crypto/cipher/e_ssl3.c \
    third_party/boringssl/crypto/cipher/e_tls.c \
    third_party/boringssl/crypto/cipher/tls_cbc.c \
    third_party/boringssl/crypto/cmac/cmac.c \
    third_party/boringssl/crypto/conf/conf.c \
    third_party/boringssl/crypto/cpu-aarch64-linux.c \
    third_party/boringssl/crypto/cpu-arm-linux.c \
    third_party/boringssl/crypto/cpu-arm.c \
    third_party/boringssl/crypto/cpu-intel.c \
    third_party/boringssl/crypto/cpu-ppc64le.c \
    third_party/boringssl/crypto/crypto.c \
    third_party/boringssl/crypto/curve25519/curve25519.c \
    third_party/boringssl/crypto/curve25519/spake25519.c \
    third_party/boringssl/crypto/curve25519/x25519-x86_64.c \
    third_party/boringssl/crypto/des/des.c \
    third_party/boringssl/crypto/dh/check.c \
    third_party/boringssl/crypto/dh/dh.c \
    third_party/boringssl/crypto/dh/dh_asn1.c \
    third_party/boringssl/crypto/dh/params.c \
    third_party/boringssl/crypto/digest/digest.c \
    third_party/boringssl/crypto/digest/digests.c \
    third_party/boringssl/crypto/dsa/dsa.c \
    third_party/boringssl/crypto/dsa/dsa_asn1.c \
    third_party/boringssl/crypto/ec/ec.c \
    third_party/boringssl/crypto/ec/ec_asn1.c \
    third_party/boringssl/crypto/ec/ec_key.c \
    third_party/boringssl/crypto/ec/ec_montgomery.c \
    third_party/boringssl/crypto/ec/oct.c \
    third_party/boringssl/crypto/ec/p224-64.c \
    third_party/boringssl/crypto/ec/p256-64.c \
    third_party/boringssl/crypto/ec/p256-x86_64.c \
    third_party/boringssl/crypto/ec/simple.c \
    third_party/boringssl/crypto/ec/util-64.c \
    third_party/boringssl/crypto/ec/wnaf.c \
    third_party/boringssl/crypto/ecdh/ecdh.c \
    third_party/boringssl/crypto/ecdsa/ecdsa.c \
    third_party/boringssl/crypto/ecdsa/ecdsa_asn1.c \
    third_party/boringssl/crypto/engine/engine.c \
    third_party/boringssl/crypto/err/err.c \
    third_party/boringssl/crypto/evp/digestsign.c \
    third_party/boringssl/crypto/evp/evp.c \
    third_party/boringssl/crypto/evp/evp_asn1.c \
    third_party/boringssl/crypto/evp/evp_ctx.c \
    third_party/boringssl/crypto/evp/p_dsa_asn1.c \
    third_party/boringssl/crypto/evp/p_ec.c \
    third_party/boringssl/crypto/evp/p_ec_asn1.c \
    third_party/boringssl/crypto/evp/p_rsa.c \
    third_party/boringssl/crypto/evp/p_rsa_asn1.c \
    third_party/boringssl/crypto/evp/pbkdf.c \
    third_party/boringssl/crypto/evp/print.c \
    third_party/boringssl/crypto/evp/sign.c \
    third_party/boringssl/crypto/ex_data.c \
    third_party/boringssl/crypto/hkdf/hkdf.c \
    third_party/boringssl/crypto/hmac/hmac.c \
    third_party/boringssl/crypto/lhash/lhash.c \
    third_party/boringssl/crypto/md4/md4.c \
    third_party/boringssl/crypto/md5/md5.c \
    third_party/boringssl/crypto/mem.c \
    third_party/boringssl/crypto/modes/cbc.c \
    third_party/boringssl/crypto/modes/cfb.c \
    third_party/boringssl/crypto/modes/ctr.c \
    third_party/boringssl/crypto/modes/gcm.c \
    third_party/boringssl/crypto/modes/ofb.c \
    third_party/boringssl/crypto/modes/polyval.c \
    third_party/boringssl/crypto/obj/obj.c \
    third_party/boringssl/crypto/obj/obj_xref.c \
    third_party/boringssl/crypto/pem/pem_all.c \
    third_party/boringssl/crypto/pem/pem_info.c \
    third_party/boringssl/crypto/pem/pem_lib.c \
    third_party/boringssl/crypto/pem/pem_oth.c \
    third_party/boringssl/crypto/pem/pem_pk8.c \
    third_party/boringssl/crypto/pem/pem_pkey.c \
    third_party/boringssl/crypto/pem/pem_x509.c \
    third_party/boringssl/crypto/pem/pem_xaux.c \
    third_party/boringssl/crypto/pkcs8/p5_pbev2.c \
    third_party/boringssl/crypto/pkcs8/p8_pkey.c \
    third_party/boringssl/crypto/pkcs8/pkcs8.c \
    third_party/boringssl/crypto/poly1305/poly1305.c \
    third_party/boringssl/crypto/poly1305/poly1305_arm.c \
    third_party/boringssl/crypto/poly1305/poly1305_vec.c \
    third_party/boringssl/crypto/pool/pool.c \
    third_party/boringssl/crypto/rand/deterministic.c \
    third_party/boringssl/crypto/rand/fuchsia.c \
    third_party/boringssl/crypto/rand/rand.c \
    third_party/boringssl/crypto/rand/urandom.c \
    third_party/boringssl/crypto/rand/windows.c \
    third_party/boringssl/crypto/rc4/rc4.c \
    third_party/boringssl/crypto/refcount_c11.c \
    third_party/boringssl/crypto/refcount_lock.c \
    third_party/boringssl/crypto/rsa/blinding.c \
    third_party/boringssl/crypto/rsa/padding.c \
    third_party/boringssl/crypto/rsa/rsa.c \
    third_party/boringssl/crypto/rsa/rsa_asn1.c \
    third_party/boringssl/crypto/rsa/rsa_impl.c \
    third_party/boringssl/crypto/sha/sha1-altivec.c \
    third_party/boringssl/crypto/sha/sha1.c \
    third_party/boringssl/crypto/sha/sha256.c \
    third_party/boringssl/crypto/sha/sha512.c \
    third_party/boringssl/crypto/stack/stack.c \
    third_party/boringssl/crypto/thread.c \
    third_party/boringssl/crypto/thread_none.c \
    third_party/boringssl/crypto/thread_pthread.c \
    third_party/boringssl/crypto/thread_win.c \
    third_party/boringssl/crypto/x509/a_digest.c \
    third_party/boringssl/crypto/x509/a_sign.c \
    third_party/boringssl/crypto/x509/a_strex.c \
    third_party/boringssl/crypto/x509/a_verify.c \
    third_party/boringssl/crypto/x509/algorithm.c \
    third_party/boringssl/crypto/x509/asn1_gen.c \
    third_party/boringssl/crypto/x509/by_dir.c \
    third_party/boringssl/crypto/x509/by_file.c \
    third_party/boringssl/crypto/x509/i2d_pr.c \
    third_party/boringssl/crypto/x509/pkcs7.c \
    third_party/boringssl/crypto/x509/rsa_pss.c \
    third_party/boringssl/crypto/x509/t_crl.c \
    third_party/boringssl/crypto/x509/t_req.c \
    third_party/boringssl/crypto/x509/t_x509.c \
    third_party/boringssl/crypto/x509/t_x509a.c \
    third_party/boringssl/crypto/x509/x509.c \
    third_party/boringssl/crypto/x509/x509_att.c \
    third_party/boringssl/crypto/x509/x509_cmp.c \
    third_party/boringssl/crypto/x509/x509_d2.c \
    third_party/boringssl/crypto/x509/x509_def.c \
    third_party/boringssl/crypto/x509/x509_ext.c \
    third_party/boringssl/crypto/x509/x509_lu.c \
    third_party/boringssl/crypto/x509/x509_obj.c \
    third_party/boringssl/crypto/x509/x509_r2x.c \
    third_party/boringssl/crypto/x509/x509_req.c \
    third_party/boringssl/crypto/x509/x509_set.c \
    third_party/boringssl/crypto/x509/x509_trs.c \
    third_party/boringssl/crypto/x509/x509_txt.c \
    third_party/boringssl/crypto/x509/x509_v3.c \
    third_party/boringssl/crypto/x509/x509_vfy.c \
    third_party/boringssl/crypto/x509/x509_vpm.c \
    third_party/boringssl/crypto/x509/x509cset.c \
    third_party/boringssl/crypto/x509/x509name.c \
    third_party/boringssl/crypto/x509/x509rset.c \
    third_party/boringssl/crypto/x509/x509spki.c \
    third_party/boringssl/crypto/x509/x509type.c \
    third_party/boringssl/crypto/x509/x_algor.c \
    third_party/boringssl/crypto/x509/x_all.c \
    third_party/boringssl/crypto/x509/x_attrib.c \
    third_party/boringssl/crypto/x509/x_crl.c \
    third_party/boringssl/crypto/x509/x_exten.c \
    third_party/boringssl/crypto/x509/x_info.c \
    third_party/boringssl/crypto/x509/x_name.c \
    third_party/boringssl/crypto/x509/x_pkey.c \
    third_party/boringssl/crypto/x509/x_pubkey.c \
    third_party/boringssl/crypto/x509/x_req.c \
    third_party/boringssl/crypto/x509/x_sig.c \
    third_party/boringssl/crypto/x509/x_spki.c \
    third_party/boringssl/crypto/x509/x_val.c \
    third_party/boringssl/crypto/x509/x_x509.c \
    third_party/boringssl/crypto/x509/x_x509a.c \
    third_party/boringssl/crypto/x509v3/pcy_cache.c \
    third_party/boringssl/crypto/x509v3/pcy_data.c \
    third_party/boringssl/crypto/x509v3/pcy_lib.c \
    third_party/boringssl/crypto/x509v3/pcy_map.c \
    third_party/boringssl/crypto/x509v3/pcy_node.c \
    third_party/boringssl/crypto/x509v3/pcy_tree.c \
    third_party/boringssl/crypto/x509v3/v3_akey.c \
    third_party/boringssl/crypto/x509v3/v3_akeya.c \
    third_party/boringssl/crypto/x509v3/v3_alt.c \
    third_party/boringssl/crypto/x509v3/v3_bcons.c \
    third_party/boringssl/crypto/x509v3/v3_bitst.c \
    third_party/boringssl/crypto/x509v3/v3_conf.c \
    third_party/boringssl/crypto/x509v3/v3_cpols.c \
    third_party/boringssl/crypto/x509v3/v3_crld.c \
    third_party/boringssl/crypto/x509v3/v3_enum.c \
    third_party/boringssl/crypto/x509v3/v3_extku.c \
    third_party/boringssl/crypto/x509v3/v3_genn.c \
    third_party/boringssl/crypto/x509v3/v3_ia5.c \
    third_party/boringssl/crypto/x509v3/v3_info.c \
    third_party/boringssl/crypto/x509v3/v3_int.c \
    third_party/boringssl/crypto/x509v3/v3_lib.c \
    third_party/boringssl/crypto/x509v3/v3_ncons.c \
    third_party/boringssl/crypto/x509v3/v3_pci.c \
    third_party/boringssl/crypto/x509v3/v3_pcia.c \
    third_party/boringssl/crypto/x509v3/v3_pcons.c \
    third_party/boringssl/crypto/x509v3/v3_pku.c \
    third_party/boringssl/crypto/x509v3/v3_pmaps.c \
    third_party/boringssl/crypto/x509v3/v3_prn.c \
    third_party/boringssl/crypto/x509v3/v3_purp.c \
    third_party/boringssl/crypto/x509v3/v3_skey.c \
    third_party/boringssl/crypto/x509v3/v3_sxnet.c \
    third_party/boringssl/crypto/x509v3/v3_utl.c \
    third_party/boringssl/ssl/bio_ssl.c \
    third_party/boringssl/ssl/custom_extensions.c \
    third_party/boringssl/ssl/d1_both.c \
    third_party/boringssl/ssl/d1_lib.c \
    third_party/boringssl/ssl/d1_pkt.c \
    third_party/boringssl/ssl/d1_srtp.c \
    third_party/boringssl/ssl/dtls_method.c \
    third_party/boringssl/ssl/dtls_record.c \
    third_party/boringssl/ssl/handshake_client.c \
    third_party/boringssl/ssl/handshake_server.c \
    third_party/boringssl/ssl/s3_both.c \
    third_party/boringssl/ssl/s3_lib.c \
    third_party/boringssl/ssl/s3_pkt.c \
    third_party/boringssl/ssl/ssl_aead_ctx.c \
    third_party/boringssl/ssl/ssl_asn1.c \
    third_party/boringssl/ssl/ssl_buffer.c \
    third_party/boringssl/ssl/ssl_cert.c \
    third_party/boringssl/ssl/ssl_cipher.c \
    third_party/boringssl/ssl/ssl_ecdh.c \
    third_party/boringssl/ssl/ssl_file.c \
    third_party/boringssl/ssl/ssl_lib.c \
    third_party/boringssl/ssl/ssl_privkey.c \
    third_party/boringssl/ssl/ssl_privkey_cc.cc \
    third_party/boringssl/ssl/ssl_session.c \
    third_party/boringssl/ssl/ssl_stat.c \
    third_party/boringssl/ssl/ssl_transcript.c \
    third_party/boringssl/ssl/ssl_x509.c \
    third_party/boringssl/ssl/t1_enc.c \
    third_party/boringssl/ssl/t1_lib.c \
    third_party/boringssl/ssl/tls13_both.c \
    third_party/boringssl/ssl/tls13_client.c \
    third_party/boringssl/ssl/tls13_enc.c \
    third_party/boringssl/ssl/tls13_server.c \
    third_party/boringssl/ssl/tls_method.c \
    third_party/boringssl/ssl/tls_record.c \
    , $ext_shared, , -fvisibility=hidden \
    -DOPENSSL_NO_ASM -D_GNU_SOURCE -DWIN32_LEAN_AND_MEAN \
    -D_HAS_EXCEPTIONS=0 -DNOMINMAX -DGRPC_ARES=0)

  PHP_ADD_BUILD_DIR($ext_builddir/src/php/ext/grpc)

  PHP_ADD_BUILD_DIR($ext_builddir/src/boringssl)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/census)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/lb_policy)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/lb_policy/grpclb)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/lb_policy/grpclb/proto/grpc/lb/v1)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/lb_policy/pick_first)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/lb_policy/round_robin)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/resolver/dns/c_ares)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/resolver/dns/native)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/resolver/fake)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/client_channel/resolver/sockaddr)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/deadline)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/http)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/http/client)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/http/message_compress)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/http/server)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/load_reporting)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/max_age)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/message_size)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/filters/workarounds)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/alpn)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/client)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/client/insecure)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/client/secure)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/server)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/server/insecure)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/server/secure)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/chttp2/transport)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/ext/transport/inproc)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/backoff)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/channel)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/compression)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/debug)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/http)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/iomgr)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/json)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/profiling)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/context)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/composite)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/fake)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/google_default)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/iam)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/jwt)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/oauth2)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/plugin)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/credentials/ssl)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/transport)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/security/util)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/slice)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/support)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/surface)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/lib/transport)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/plugin_registry)
  PHP_ADD_BUILD_DIR($ext_builddir/src/core/tsi)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/aes)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/asn1)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/base64)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/bio)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/bn)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/bn/asm)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/buf)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/bytestring)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/chacha)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/cipher)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/cmac)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/conf)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/curve25519)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/des)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/dh)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/digest)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/dsa)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/ec)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/ecdh)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/ecdsa)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/engine)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/err)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/evp)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/hkdf)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/hmac)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/lhash)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/md4)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/md5)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/modes)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/obj)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/pem)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/pkcs8)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/poly1305)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/pool)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/rand)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/rc4)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/rsa)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/sha)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/stack)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/x509)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/crypto/x509v3)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/boringssl/ssl)
  PHP_ADD_BUILD_DIR($ext_builddir/third_party/nanopb)
fi
