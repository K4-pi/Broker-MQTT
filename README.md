# Broker-MQTT

MQTT 3.1.1 broker written in C++20.

## Technologies

| Piece | Used for |
| --- | --- |
| C++20 | Broker implementation |
| TCP sockets | Client connections |
| epoll | Notifying the broker when a client connects or has data |
| io_uring (liburing) | Asynchronous reading/writing MQTT packets |
| Boost.Threadpool | Worker threads that start a read for each new connection |
| Boost.Tokenizer | Splitting topic paths into levels |
| GNU Make, g++ | Build |

## Supported MQTT 3.1.1 features

| Feature | Supported |
| --- | --- |
| CONNECT / CONNACK | Yes |
| PINGREQ / PINGRESP | Yes |
| DISCONNECT | Yes |
| PUBLISH, QoS 0, 1 and 2 | Yes |
| PUBACK, PUBREC, PUBREL, PUBCOMP | Yes |
| RETAIN, including replay to a new subscriber | Yes |
| SUBSCRIBE / SUBACK | Yes |
| UNSUBSCRIBE / UNSUBACK | Yes |
| Topic filters `+` and `#` | Yes |
| Idle clients dropped after 10 seconds | Yes |
| Username, password, or TLS | No |
| Will messages | No |
| Sessions kept after disconnect | No |
| MQTT 5 | No |

## Build

```bash
make
```

## License

MIT, see `LICENSE`.

The vendored thread pool in `include/threadpool.hpp` and `include/threadpool/` is Boost Software License 1.0. See `THIRD_PARTY_NOTICES.md` and `LICENSES/BSL-1.0.txt`.
