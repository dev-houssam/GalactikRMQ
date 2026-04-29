#include "galactik_rmq.h"
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define QUEUE_SIZE 1000
#define MSG_MAX_LEN 8192

typedef struct {
    char* items[QUEUE_SIZE];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
} ThreadSafeQueue;

static void queue_init(ThreadSafeQueue* q) {
    memset(q->items, 0, sizeof(q->items));
    q->front = 0;
    q->rear = 0;
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
}

static void queue_push(ThreadSafeQueue* q, const char* msg) {
    pthread_mutex_lock(&q->mutex);
    if (q->count < QUEUE_SIZE) {
        q->items[q->rear] = strdup(msg);
        q->rear = (q->rear + 1) % QUEUE_SIZE;
        q->count++;
    }
    pthread_mutex_unlock(&q->mutex);
}

static char* queue_pop(ThreadSafeQueue* q) {
    pthread_mutex_lock(&q->mutex);
    char* msg = NULL;
    if (q->count > 0) {
        msg = q->items[q->front];
        q->items[q->front] = NULL;
        q->front = (q->front + 1) % QUEUE_SIZE;
        q->count--;
    }
    pthread_mutex_unlock(&q->mutex);
    return msg;
}

static void queue_free(ThreadSafeQueue* q) {
    pthread_mutex_lock(&q->mutex);
    for (int i = 0; i < QUEUE_SIZE; i++) {
        free(q->items[i]);
    }
    pthread_mutex_unlock(&q->mutex);
    pthread_mutex_destroy(&q->mutex);
}

struct GalactikRMQ {
    char exchange[256];
    char binding_key[256];
    char queue[256];
    char hostname[256];
    int port;
    char vhost[256];
    char username[256];
    char password[256];

    amqp_connection_state_t conn;
    ThreadSafeQueue msg_queue;
    pthread_t consumer_thread;
    volatile int running;
};

static void* consumer_loop(void* arg) {
    GalactikRMQ* grmq = (GalactikRMQ*)arg;
    amqp_envelope_t envelope;
    struct timeval timeout = {1, 0};

    while (grmq->running) {
        amqp_maybe_release_buffers(grmq->conn);
        amqp_rpc_reply_t ret = amqp_consume_message(grmq->conn, &envelope, &timeout, 0);

        if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
            char* msg = malloc(envelope.message.body.len + 1);
            if (msg) {
                memcpy(msg, envelope.message.body.bytes, envelope.message.body.len);
                msg[envelope.message.body.len] = '\0';
                queue_push(&grmq->msg_queue, msg);
                free(msg);
            }
            amqp_destroy_envelope(&envelope);
        } else if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
                   ret.library_error == AMQP_STATUS_TIMEOUT) {
            continue;
        } else {
            break;
        }
    }
    return NULL;
}

GalactikRMQ* galactik_rmq_new(void) {
    GalactikRMQ* grmq = calloc(1, sizeof(GalactikRMQ));
    if (!grmq) return NULL;

    grmq->port = 5672;
    snprintf(grmq->hostname, sizeof(grmq->hostname), "localhost");
    snprintf(grmq->vhost, sizeof(grmq->vhost), "/");
    snprintf(grmq->username, sizeof(grmq->username), "guest");
    snprintf(grmq->password, sizeof(grmq->password), "guest");

    queue_init(&grmq->msg_queue);
    return grmq;
}

void galactik_rmq_set_exchange(GalactikRMQ* grmq, const char* exchange, const char* binding_key, const char* queue) {
    if (exchange) snprintf(grmq->exchange, sizeof(grmq->exchange), "%s", exchange);
    if (binding_key) snprintf(grmq->binding_key, sizeof(grmq->binding_key), "%s", binding_key);
    if (queue) snprintf(grmq->queue, sizeof(grmq->queue), "%s", queue);
}

void galactik_rmq_set_connection(GalactikRMQ* grmq, const char* hostname, int port) {
    if (hostname) snprintf(grmq->hostname, sizeof(grmq->hostname), "%s", hostname);
    if (port > 0) grmq->port = port;
}

void galactik_rmq_set_login(GalactikRMQ* grmq, const char* vhost, const char* username, const char* password) {
    if (vhost) snprintf(grmq->vhost, sizeof(grmq->vhost), "%s", vhost);
    if (username) snprintf(grmq->username, sizeof(grmq->username), "%s", username);
    if (password) snprintf(grmq->password, sizeof(grmq->password), "%s", password);
}

int galactik_rmq_start_consumer(GalactikRMQ* grmq) {
    grmq->conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(grmq->conn);
    if (!socket) return -1;

    if (amqp_socket_open(socket, grmq->hostname, grmq->port)) return -1;

    amqp_rpc_reply_t reply = amqp_login(grmq->conn, grmq->vhost, 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, grmq->username, grmq->password);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) return -1;

    amqp_channel_open(grmq->conn, 1);
    reply = amqp_get_rpc_reply(grmq->conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) return -1;

    amqp_queue_declare(grmq->conn, 1, amqp_cstring_bytes(grmq->queue), 0, 0, 0, 1, amqp_empty_table);
    reply = amqp_get_rpc_reply(grmq->conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) return -1;

    amqp_queue_bind(grmq->conn, 1, amqp_cstring_bytes(grmq->queue), amqp_cstring_bytes(grmq->exchange), amqp_cstring_bytes(grmq->binding_key), amqp_empty_table);
    reply = amqp_get_rpc_reply(grmq->conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) return -1;

    amqp_basic_consume(grmq->conn, 1, amqp_cstring_bytes(grmq->queue), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    reply = amqp_get_rpc_reply(grmq->conn);
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) return -1;

    grmq->running = 1;
    pthread_create(&grmq->consumer_thread, NULL, consumer_loop, grmq);
    return 0;
}

char* galactik_rmq_get_message(GalactikRMQ* grmq) {
    return queue_pop(&grmq->msg_queue);
}

void galactik_rmq_stop(GalactikRMQ* grmq) {
    grmq->running = 0;
    pthread_join(grmq->consumer_thread, NULL);
    amqp_channel_close(grmq->conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(grmq->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(grmq->conn);
}

void galactik_rmq_free(GalactikRMQ* grmq) {
    if (!grmq) return;
    galactik_rmq_stop(grmq);
    queue_free(&grmq->msg_queue);
    free(grmq);
}
