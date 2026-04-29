#ifndef GALACTIK_RMQ_CPP_HPP
#define GALACTIK_RMQ_CPP_HPP

#include <string>
#include <memory>
#include "galactik_rmq.h"

class GalactikRMQ {
public:
    GalactikRMQ() : handle(galactik_rmq_new(), galactik_rmq_free) {}

    void setExchange(const std::string& exchange, const std::string& bindingKey, const std::string& queue) {
        galactik_rmq_set_exchange(handle.get(), exchange.c_str(), bindingKey.c_str(), queue.c_str());
    }

    void setConnection(const std::string& hostname, int port) {
        galactik_rmq_set_connection(handle.get(), hostname.c_str(), port);
    }

    void setLogin(const std::string& vhost, const std::string& username, const std::string& password) {
        galactik_rmq_set_login(handle.get(), vhost.c_str(), username.c_str(), password.c_str());
    }

    bool startConsumer() {
        return galactik_rmq_start_consumer(handle.get()) == 0;
    }

    std::string consumeMessage() {
        char* msg = galactik_rmq_get_message(handle.get());
        if (!msg) return "";
        std::string result(msg);
        free(msg);
        return result;
    }

    void stop() {
        galactik_rmq_stop(handle.get());
    }

private:
    struct GalactikRMQDeleter {
        void operator()(GalactikRMQ* ptr) const { galactik_rmq_free(ptr); }
    };
    std::unique_ptr<GalactikRMQ, GalactikRMQDeleter> handle;
};

#endif
