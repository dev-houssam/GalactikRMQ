#include "galactik_rmq_cpp.hpp"
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>
#include <chrono>

using json = nlohmann::json;

int main() {
    GalactikRMQ rmq;

    rmq.setExchange("amq.topic", "sensor.data", "my_queue");
    rmq.setConnection("localhost", 5672);
    rmq.setLogin("/", "guest", "guest");

    if (!rmq.startConsumer()) {
        std::cerr << "Failed to start consumer" << std::endl;
        return 1;
    }

    std::cout << "Listening for messages..." << std::endl;

    while (true) {
        std::string msg = rmq.consumeMessage();
        if (!msg.empty()) {
            try {
                auto data = json::parse(msg);
                std::string sensorId = data.value("sensor_id", "unknown");
                std::string message  = data.value("message", "");
                std::string timestamp= data.value("timestamp", "");

                std::cout << "Sensor: " << sensorId
                          << " | Message: " << message
                          << " | Time: " << timestamp << std::endl;

            } catch (const std::exception& e) {
                std::cerr << "JSON parse error: " << e.what() << std::endl;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return 0;
}
