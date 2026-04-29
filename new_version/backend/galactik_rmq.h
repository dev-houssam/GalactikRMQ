#ifndef GALACTIK_RMQ_H
#define GALACTIK_RMQ_H

#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct GalactikRMQ GalactikRMQ;

// Create & destroy
GalactikRMQ* galactik_rmq_new(void);
void galactik_rmq_free(GalactikRMQ* grmq);

// Configuration
void galactik_rmq_set_exchange(GalactikRMQ* grmq, const char* exchange, const char* binding_key, const char* queue);
void galactik_rmq_set_connection(GalactikRMQ* grmq, const char* hostname, int port);
void galactik_rmq_set_login(GalactikRMQ* grmq, const char* vhost, const char* username, const char* password);

// Start consumer thread
int galactik_rmq_start_consumer(GalactikRMQ* grmq);

// Get last consumed message (caller must free)
char* galactik_rmq_get_message(GalactikRMQ* grmq);

// Stop consumer
void galactik_rmq_stop(GalactikRMQ* grmq);

#ifdef __cplusplus
}
#endif

#endif
