# GalactikRMQ - RabbitMQ Consumer Library

A lightweight C/C++ library for consuming messages from RabbitMQ with a thread-safe queue and modern C++ wrapper.

## Project Structure

```
GalactikRMQ/
├── backend/           # C core library
│   ├── galactik_rmq.h
│   ├── galactik_rmq.c
│   └── CMakeLists.txt
├── frontend/          # C++ wrapper and example
│   ├── galactik_rmq_cpp.hpp
│   ├── main.cpp
│   └── CMakeLists.txt
└── CMakeLists.txt
```

## Dependencies

- rabbitmq-c (librabbitmq)
- pthread
- nlohmann/json (for C++ example only)

### Install dependencies (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install librabbitmq-dev nlohmann-json3-dev
```

## Building

```bash
mkdir build && cd build
cmake ..
make
```

## Backend C API

### Basic usage

```c
#include "galactik_rmq.h"

int main() {
    GalactikRMQ* rmq = galactik_rmq_new();

    galactik_rmq_set_exchange(rmq, "amq.topic", "sensor.data", "my_queue");
    galactik_rmq_set_connection(rmq, "localhost", 5672);
    galactik_rmq_set_login(rmq, "/", "guest", "guest");

    if (galactik_rmq_start_consumer(rmq) == 0) {
        char* msg;
        while ((msg = galactik_rmq_get_message(rmq)) != NULL) {
            printf("Received: %s\n", msg);
            free(msg);
        }
    }

    galactik_rmq_free(rmq);
    return 0;
}
```

### API Reference

| Function | Description |
|----------|-------------|
| `galactik_rmq_new()` | Create a new instance |
| `galactik_rmq_free()` | Destroy instance and free resources |
| `galactik_rmq_set_exchange()` | Configure exchange, binding key, and queue |
| `galactik_rmq_set_connection()` | Set RabbitMQ host and port |
| `galactik_rmq_set_login()` | Set vhost, username, password |
| `galactik_rmq_start_consumer()` | Start consumer thread |
| `galactik_rmq_get_message()` | Get next message (caller must free) |
| `galactik_rmq_stop()` | Stop consumer thread |

## Frontend C++ API

### Basic usage

```cpp
#include "galactik_rmq_cpp.hpp"
#include <iostream>

int main() {
    GalactikRMQ rmq;

    rmq.setExchange("amq.topic", "sensor.data", "my_queue");
    rmq.setConnection("localhost", 5672);
    rmq.setLogin("/", "guest", "guest");

    if (rmq.startConsumer()) {
        while (true) {
            std::string msg = rmq.consumeMessage();
            if (!msg.empty()) {
                std::cout << msg << std::endl;
            }
        }
    }

    return 0;
}
```

### C++ API Reference

| Method | Description |
|--------|-------------|
| `setExchange(exchange, bindingKey, queue)` | Configure exchange binding |
| `setConnection(hostname, port)` | Set connection parameters |
| `setLogin(vhost, username, password)` | Set authentication |
| `startConsumer()` | Start consumer thread, returns bool |
| `consumeMessage()` | Get next message as string (non-blocking) |
| `stop()` | Stop consumer thread |

## Example with JSON (main.cpp)

The provided `main.cpp` demonstrates consuming JSON messages:

```cpp
GalactikRMQ rmq;
rmq.setExchange("amq.topic", "sensor.data", "sensor_queue");
rmq.setConnection("localhost", 5672);
rmq.setLogin("/", "guest", "guest");

rmq.startConsumer();

while (true) {
    std::string msg = rmq.consumeMessage();
    if (!msg.empty()) {
        auto data = json::parse(msg);
        std::string sensorId = data.value("sensor_id", "unknown");
        std::string message  = data.value("message", "");
        // Process your data...
    }
}
```

## Thread Safety

- The internal message queue is protected by a mutex
- Consumer runs in a dedicated thread
- `consumeMessage()` and `get_message()` are thread-safe

## Queue Configuration

The default circular queue holds up to 1000 messages. Each message can be up to 8192 bytes. Modify `QUEUE_SIZE` and `MSG_MAX_LEN` in `galactik_rmq.c` if needed.

## Running Tests

Publish a test message using RabbitMQ command line:

```bash
rabbitmqadmin publish exchange=amq.topic routing_key=sensor.data payload='{"sensor_id":"test","message":"hello","timestamp":"2024-01-01T12:00:00"}'
```

## Limitations

- Automatic reconnection not implemented
- No message acknowledgment (auto-ack mode)
- Single queue per instance

## License

MIT
