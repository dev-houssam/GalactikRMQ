#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define MESSAGE_UNIT 10

#define MAX_SIZE 4056*MESSAGE_UNIT

// Defining the Queue structure
typedef struct {
    char*  items[MAX_SIZE];
    int front;
    int rear;
} Queue;


// Function to initialize the queue
void initializeQueue(Queue* q)
{
    q->front = -1;
    q->rear = 0;
}

// Function to check if the queue is empty
bool isEmpty(Queue* q) { return (q->front == q->rear - 1); }

// Function to check if the queue is full
bool isFull(Queue* q) { return (q->rear == MAX_SIZE); }

// Function to add an element to the queue (Enqueue
// operation)
void enqueue(Queue* q, const char * value)
{
    if (isFull(q)) {
        printf("Queue is full\n");
        return;
    }

    int _lenValue = strlen(value) + 1 ;
    // Allocation memory
    q->items[q->rear] = (char *) malloc( sizeof(char) * _lenValue );
    if(q->items[q->rear] == NULL){
	fprintf(stderr, "Impossible to perform malloc on enqueue : REAR=%d, FRONT=%d\n", q->rear, q->front);
    	return;
    }
    memcpy(q->items[q->rear], value, _lenValue);
    q->rear++;
}

// Function to remove an element from the queue (Dequeue
// operation)
void dequeue(Queue* q)
{
    if (isEmpty(q)) {
        printf("Queue is empty\n");
        return;
    }
    q->front++;
}

// Function to get the element at the front of the queue
// (Peek operation)
char * peek(Queue* q)
{
    if (isEmpty(q)) {
        printf("Queue is empty\n");
        return -1; // return some default value or handle
                   // error differently
    }
    return q->items[q->front + 1];
}

// Function to print the current queue
void printQueue(Queue* q)
{
    if (isEmpty(q)) {
        printf("Queue is empty\n");
        return;
    }

    printf("Current Queue: ");
    for (int i = q->front + 1; i < q->rear; i++) {
        printf("%s ", q->items[i]);
    }
    printf("\n");
}


//// END HANDLING QUEUE

// START TO HANDLE RABBIT CONSUMER
//

typedef struct {
  char * exchange;
  char * bindingKey;
  char * queue;
} ConfigurationExchange;

typedef struct {
  amqp_socket_t socket;
  char *hostname;
  int   port;
} ConfigurationConnection;

typedef struct {
  amqp_connection_state_t conn;
  char * host;
  int channel_max;
  int frame_max;
  int heartbeat;
  amqp_sasl_method_enum sasl_method;
  char * username;
  char * password;
} ConfigurationLogin;

typedef struct {
  char * loginExceptionMessage;
  char * openingChannelExceptionMessage;
  char * queueExceptionMessage;
  char * bindingExceptionMessage;
  char * consumingExceptionMessage;
  char * closingChannelExceptionMessage;
  char * closingConnectionExceptionMessage;
  char * endingConnectionExceptionMessage;
} ExceptionHandling;


typedef struct {
  ConfigurationExchange * conf_exchange;
  ConfigurationConnection * conf_conn;
  ConfigurationLogin * conf_login;
  ExceptionHandling * except;
  Queue * consumingQueue;
} GalactikRQM;


typedef enum {
  GRQM_EXIT_FAILURE = -1
} GalactikRQM_EXIT_CODE;


GalactikRQM * create_GalactikRQM_Instance_private(){
  GalactikRQM * GRQM = (GalactikRQM * ) malloc(sizeof (GalactikRQM));
  if (GRQM == NULL)
  {
    fprintf(stderr, "%s\n", "Unable to allocate memory : GalactikRQM cannot be created");
    exit(GRQM_EXIT_FAILURE);
  }
  return GRQM;
}

// Public

ConfigurationExchange * 
create_GalactikRQM_Configuration_Exchange_private
( const char * exchange_name,
  const char * bindingKey,
  const char *queuename){
    ConfigurationExchange * ce = (ConfigurationExchange * ) malloc(sizeof(ConfigurationExchange));
    if(ce == NULL) return NULL;
    ce->exchange = (char *) malloc(sizeof(char) * ( strlen(exchange_name) + 1 ));
    if(ce->exchange == NULL) return NULL;
    memset(ce->exchange, 0, strlen(exchange_name) + 1 );
    memcpy(ce->exchange, exchange_name, strlen(exchange_name));
    //..
    ce->bindingKey = (char *) malloc(sizeof(char) * ( strlen(bindingKey) + 1 ));
    if(ce->bindingKey == NULL) return NULL;
    memset(ce->bindingKey, 0, strlen(bindingKey) + 1 );
    memcpy(ce->bindingKey, bindingKey, strlen(bindingKey));    
    //..
    ce->queue = (char *) malloc(sizeof(char) * ( strlen(queuename) + 1 ));
    if(ce->queue == NULL) return NULL;
    memset(ce->queue, 0, strlen(queuename) + 1 );
    memcpy(ce->queue, queuename, strlen(queuename)); 


  return ce;
}

ConfigurationConnection * 
create_GalactikRQM_ConfigurationConnection_private
( const char * hostname,
  int port){
    ConfigurationConnection * cc = (ConfigurationConnection * ) malloc(sizeof(ConfigurationConnection));
    if(cc == NULL) return NULL;
    cc->hostname = (char *) malloc(sizeof(char) * ( strlen(hostname) + 1 ));
    if(cc->hostname == NULL) return NULL;
    memset(cc->hostname, 0, strlen(hostname) + 1 );
    memcpy(cc->hostname, hostname, strlen(hostname));
    //..
    cc->port = port;
    cc->socket = (amqp_socket_t * ) malloc(sizeof(amqp_socket_t)); //Only socket must be initialized typically : do not forget this detailled detail !
    if(NULL == cc->socket) return NULL;
  return cc;
}





ConfigurationLogin * 
create_GalactikRQM_ConfigurationLogin_private
( const char * v_host,
  int channel_max,
  int frame_max,
  int heartbeat,
  amqp_sasl_method_enum sasl_method,
  const char * username,
  const char * password){
    ConfigurationLogin * cl = (ConfigurationLogin * ) malloc(sizeof(ConfigurationLogin));
    if(cl == NULL) return NULL;
    cl->host = (char *) malloc(sizeof(char) * ( strlen(v_host) + 1 ));
    if(cl->host == NULL) return NULL;
    memset(cl->host, 0, strlen(v_host) + 1 );
    memcpy(cl->host, v_host, strlen(v_host));
    //..
    cl->username = (char *) malloc(sizeof(char) * ( strlen(username) + 1 ));
    if(cl->username == NULL) return NULL;
    memset(cl->username, 0, strlen(username) + 1 );
    memcpy(cl->username, username, strlen(username));    
    //..
    cl->password = (char *) malloc(sizeof(char) * ( strlen(password) + 1 ));
    if(cl->password == NULL) return NULL;
    memset(cl->password, 0, strlen(password) + 1 );
    memcpy(cl->password, password, strlen(password)); 
    //.....
    (void) cl->conn; 
    cl->channel_max = channel_max;
    cl->frame_max = frame_max;
    cl->heartbeat = heartbeat;
    cl->sasl_method = sasl_method;

  return cl;
}

ExceptionHandling * 
create_GalactikRQM_ExceptionHandling_private
( const char * loginExceptionMessage,
  const char * openingChannelExceptionMessage,
  const char * queueExceptionMessage,
  const char * bindingExceptionMessage,
  const char * consumingExceptionMessage,
  const char * closingChannelExceptionMessage,
  const char * closingConnectionExceptionMessage,
  const char * endingConnectionExceptionMessage){
    ExceptionHandling * eh = (ExceptionHandling * ) malloc(sizeof(ExceptionHandling));
    if(eh == NULL) return NULL;
    eh->loginExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(loginExceptionMessage) + 1 ));
    if(eh->loginExceptionMessage == NULL) return NULL;
    memset(eh->loginExceptionMessage, 0, strlen(loginExceptionMessage) + 1 );
    memcpy(eh->loginExceptionMessage, loginExceptionMessage, strlen(loginExceptionMessage));
    //..
    eh->openingChannelExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(openingChannelExceptionMessage) + 1 ));
    if(eh->openingChannelExceptionMessage == NULL) return NULL;
    memset(eh->openingChannelExceptionMessage, 0, strlen(openingChannelExceptionMessage) + 1 );
    memcpy(eh->openingChannelExceptionMessage, openingChannelExceptionMessage, strlen(openingChannelExceptionMessage));    
    //..
    eh->queueExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(queueExceptionMessage) + 1 ));
    if(eh->queueExceptionMessage == NULL) return NULL;
    memset(eh->queueExceptionMessage, 0, strlen(queueExceptionMessage) + 1 );
    memcpy(eh->queueExceptionMessage, queueExceptionMessage, strlen(queueExceptionMessage)); 
    //.....
    //..
    eh->bindingExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(bindingExceptionMessage) + 1 ));
    if(eh->bindingExceptionMessage == NULL) return NULL;
    memset(eh->bindingExceptionMessage, 0, strlen(bindingExceptionMessage) + 1 );
    memcpy(eh->bindingExceptionMessage, bindingExceptionMessage, strlen(bindingExceptionMessage)); 
    //..
    eh->consumingExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(consumingExceptionMessage) + 1 ));
    if(eh->consumingExceptionMessage == NULL) return NULL;
    memset(eh->consumingExceptionMessage, 0, strlen(consumingExceptionMessage) + 1 );
    memcpy(eh->consumingExceptionMessage, consumingExceptionMessage, strlen(consumingExceptionMessage)); 
    //..
    eh->closingChannelExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(closingChannelExceptionMessage) + 1 ));
    if(eh->closingChannelExceptionMessage == NULL) return NULL;
    memset(eh->closingChannelExceptionMessage, 0, strlen(closingChannelExceptionMessage) + 1 );
    memcpy(eh->closingChannelExceptionMessage, closingChannelExceptionMessage, strlen(closingChannelExceptionMessage)); 
    //..
    eh->closingConnectionExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(closingConnectionExceptionMessage) + 1 ));
    if(eh->closingConnectionExceptionMessage == NULL) return NULL;
    memset(eh->closingConnectionExceptionMessage, 0, strlen(closingConnectionExceptionMessage) + 1 );
    memcpy(eh->closingConnectionExceptionMessage, closingConnectionExceptionMessage, strlen(closingConnectionExceptionMessage)); 
    //..
    eh->endingConnectionExceptionMessage = (char *) malloc(sizeof(char) * ( strlen(endingConnectionExceptionMessage) + 1 ));
    if(eh->endingConnectionExceptionMessage == NULL) return NULL;
    memset(eh->endingConnectionExceptionMessage, 0, strlen(endingConnectionExceptionMessage) + 1 );
    memcpy(eh->endingConnectionExceptionMessage, endingConnectionExceptionMessage, strlen(endingConnectionExceptionMessage)); 


  return eh; // eeehhhh !?!?!?!!
}


Queue *
create_GalactikRQM_ConsumingQueue_private(){
  Queue * q = (Queue *) malloc(sizeof(Queue));
  if(NULL == q) return NULL;
  initializeQueue(q);
  enqueue(q, ".");
  dequeue(q);
  return q;
}


GalactikRQM * _init_GalactikRQM_public(){
  GalactikRQM * _instance   = create_GalactikRQM_Instance_private();
  _instance->conf_exchange  = create_GalactikRQM_Configuration_Exchange_private(NULL, NULL, NULL);
  _instance->conf_conn      = create_GalactikRQM_ConfigurationConnection_private(NULL, 0);
  _instance->conf_login     = create_GalactikRQM_ConfigurationLogin_private(NULL, 0, 0, 0, AMQP_SASL_METHOD_PLAIN, NULL, NULL);
  _instance->except         = create_GalactikRQM_ExceptionHandling_private(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  _instance->consumingQueue = create_GalactikRQM_ConsumingQueue_private();
  return _instance;
}




void configurationExchange_GalactikRQM_public(
  GalactikRQM * _instance, 
  const char * exchange_name,
  const char * bindingKey,
  const char *queuename){
  memcpy(_instance->conf_exchange->exchange, exchange_name, strlen(exchange_name));
  memcpy(_instance->conf_exchange->bindingKey, bindingKey, strlen(bindingKey));
  memcpy(_instance->conf_exchange->queue, queuename, strlen(queuename));
}


void configurationConnection_GalactikRQM_public(GalactikRQM * _instance, const char * hostname, int port){
    memcpy(
    _instance->conf_conn->hostname, hostname, strlen(hostname));
    _instance->conf_conn->port = port;
    // Obtenir la socket
    amqp_socket_t *socket = NULL;

    _instance->conf_login->conn = amqp_new_connection();

    _instance->conf_conn->socket = amqp_tcp_socket_new(_instance->conf_login->conn);
    if (NULL == _instance->conf_conn->socket) {
      die("creating TCP socket");
    }

    amqp_connection_state_t status = amqp_socket_open(
      _instance->conf_conn->socket, 
      _instance->conf_conn->hostname, 
      _instance->conf_conn->port);
    if (status) {
      die("opening TCP socket");
    }
}

void configurationExceptionMessage_GalactikRQM_public(
  GalactikRQM * _instance, 
  const char * loginExceptionMessage,
  const char * openingChannelExceptionMessage,
  const char * queueExceptionMessage,
  const char * bindingExceptionMessage,
  const char * consumingExceptionMessage,
  const char * closingChannelExceptionMessage,
  const char * closingConnectionExceptionMessage,
  const char * endingConnectionExceptionMessage){
  memcpy(_instance->except->loginExceptionMessage, loginExceptionMessage, strlen(loginExceptionMessage));
  memcpy(_instance->except->openingChannelExceptionMessage, openingChannelExceptionMessage, strlen(openingChannelExceptionMessage));
  memcpy(_instance->except->queueExceptionMessage, queueExceptionMessage, strlen(queueExceptionMessage));
  memcpy(_instance->except->bindingExceptionMessage, bindingExceptionMessage, strlen(bindingExceptionMessage));
  memcpy(_instance->except->consumingExceptionMessage, consumingExceptionMessage, strlen(consumingExceptionMessage));
  memcpy(_instance->except->closingChannelExceptionMessage, closingChannelExceptionMessage, strlen(closingChannelExceptionMessage));
  memcpy(_instance->except->closingConnectionExceptionMessage, closingConnectionExceptionMessage, strlen(closingConnectionExceptionMessage));
  memcpy(_instance->except->endingConnectionExceptionMessage, endingConnectionExceptionMessage, strlen(endingConnectionExceptionMessage));
}


void configurationLogin_GalactikRQM_public(
  GalactikRQM * _instance, 
  const char * v_host, 
  int channel_max, 
  int frame_max,
  int heartbeat,
  amqp_sasl_method_enum sasl_method,
  const char * username,
  const char * password){
  //---
  memcpy(_instance->conf_login->host, v_host, strlen(v_host));
  memcpy(_instance->conf_login->username, username, strlen(username));
  memcpy(_instance->conf_login->password, password, strlen(password));
  _instance->conf_login->channel_max = channel_max;
  _instance->conf_login->frame_max = frame_max;
  _instance->conf_login->heartbeat = heartbeat;
  _instance->conf_login->sasl_method = sasl_method;

  amqp_rpc_reply_t ret_code = amqp_login(
    _instance->conf_login->conn, 
    _instance->conf_login->host/*/*/, 
    _instance->conf_login->channel_max/*0*/, 
    _instance->conf_login->frame_max/*131072*/, 
    _instance->conf_login->heartbeat/*0=infinite*/, 
    _instance->conf_login->sasl_method/*AMQP_SASL_METHOD_PLAIN*/, 
    _instance->conf_login->username/*"guest"*/, 
    _instance->conf_login->password/*"guest"*/);

  die_on_amqp_error(ret_code, _instance->except->loginExceptionMessage/*"Logging in"*/);
}



// Requesting Method :

int addMessageInQueue_private(GalactikRQM * _instance, const char * messageIn){
  // TODO 
  if (NULL == messageIn)
  {
    return -1;
  }
  enqueue(_instance->consumingQueue, messageIn);
  return 1;
}

// Consultation Methods : 

uint64_t getLengthConsumeQueue_private(){
  // TODO
  uint64_t getLength = 0;
  return getLength;
}

void getEnvelopeFromConsumer_public(){
  // TODO : plus tard
}

char * getCurrentMessage_public(GalactikRQM * _instance){

  /*char * ptr_message = (char *) malloc(sizeof(char)*150);
  if(NULL = ptr_message){
    return NULL;
  }*/
/*  const char * ptr_message2 = (const char * ) peek(_instance->consumingQueue);
  return ptr_message2;*/

  return (const char *) peek(_instance->consumingQueue);
}

void On_AMQP_BASIC_ACK_METHOD_private(){
  // TODO
}

void On_AMQP_BASIC_RETURN_METHOD_private(){
  // TODO
}

void On_AMQP_CHANNEL_CLOSE_METHOD_private(){
  // TODO
}

void On_AMQP_CONNECTION_CLOSE_METHOD_private(){
  // TODO
}



void retRequestHandling_private(amqp_connection_state_t conn, amqp_frame_t * frame, amqp_rpc_reply_t ret){
      if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
        if (AMQP_RESPONSE_LIBRARY_EXCEPTION == ret.reply_type &&
            AMQP_STATUS_UNEXPECTED_STATE == ret.library_error) {
          if (AMQP_STATUS_OK != amqp_simple_wait_frame(conn, &frame)) {
            return;
          }
          //Affichage du message : no 
          if (AMQP_FRAME_METHOD == frame.frame_type) {
            switch (frame.payload.method.id) {
              case AMQP_BASIC_ACK_METHOD:
                (void) On_AMQP_BASIC_ACK_METHOD_private();
                break;
              case AMQP_BASIC_RETURN_METHOD:
                (void) On_AMQP_BASIC_RETURN_METHOD_private();
                fprintf(stdout, "matching ?");
                {
                  amqp_message_t message;
                  ret = amqp_read_message(conn, frame.channel, &message, 0);
                  if (AMQP_RESPONSE_NORMAL != ret.reply_type) {
                    return;
                  }

                  amqp_destroy_message(&message);
                }
                break;
              case AMQP_CHANNEL_CLOSE_METHOD:
                (void) On_AMQP_CHANNEL_CLOSE_METHOD_private();
                return;

              case AMQP_CONNECTION_CLOSE_METHOD:
                (void) On_AMQP_CONNECTION_CLOSE_METHOD_private();
                return;

              default:
                fprintf(stderr, "An unexpected method was received %u\n",
                        frame.payload.method.id);
                return;
            }
          }
        }
    } else {
      amqp_destroy_envelope(&envelope);
    }
}




void rabbitmqConsuming_private(GalactikRQM * _instance){
  // ------------------------
  amqp_frame_t frame;

  for (;;) {
    amqp_rpc_reply_t ret;
    amqp_envelope_t envelope;  

    amqp_maybe_release_buffers(_instance->conf_login->conn);
    ret = amqp_consume_message(_instance->conf_login->conn, &envelope, NULL, 0);

    //fprintf(stdout, "Receveid h.rmq: %s\n", (const char *) envelope.message.body.bytes);

    addMessageInQueue_private(_instance, (const char * ) envelope.message.body.bytes);
    
    retRequestHandling_private(_instance->conf_login->conn, frame, ret);
  }
  //---------------------------------
}

void configurationRabbitMQ_private(GalactikRQM * info){
    //Conf
    amqp_channel_open(_instance->conf_login->conn, 1 /*WHaat ??*/);

  ret_code = amqp_get_rpc_reply(_instance->conf_login->conn);
  die_on_amqp_error(ret_code, _instance->except->openingChannelExceptionMessage /*"Opening channel"*/);

  {
    amqp_queue_declare_ok_t *r = amqp_queue_declare(_instance->conf_login->conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
    ret_code = amqp_get_rpc_reply(_instance->conf_login->conn);
    die_on_amqp_error(ret_code, _instance->except->queueExceptionMessage /*"Declaring queue"*/);

    //On a des doutes on devrai utiliser le queuename de GalactikRMQ
    memcpy(queuename, amqp_bytes_malloc_dup(r->queue), sizeof(amqp_bytes_malloc_dup(r->queue)));
    if (queuename.bytes == NULL) {
      fprintf(stderr, "Out of memory while copying queue name");
      return 1;
    }
  }

  amqp_queue_bind(
    _instance->conf_login->conn, 1, 
    _instance->conf_exchange->queue/*queuename*/, 
    amqp_cstring_bytes(_instance->conf_exchange->exchange),
    amqp_cstring_bytes(_instance->conf_exchange->bindingKey), 
    amqp_empty_table);

  ret_code = amqp_get_rpc_reply(_instance->conf_login->conn);
  die_on_amqp_error(ret_code, _instance->except->bindingExceptionMessage /*"Binding queue"*/);

  amqp_basic_consume(_instance->conf_login->conn, 
    1, _instance->conf_exchange->queue, 
    amqp_empty_bytes, 
    0, 1, 0, amqp_empty_table);
  

  ret_code = amqp_get_rpc_reply(_instance->conf_login->conn);
  die_on_amqp_error(ret_code, _instance->except->consumingExceptionMessage /*"Consuming"*/);

  fprintf(stdout, "-+-+-+-++-+-Ga+++--+laa++-++-ctiiiiik+-+-+R++M++Q-+-+-+-+++--conf-+-+\n");

}

/* Depuis C++ (classique Mais privilegions une classe): 
grmq_info = _init_GalactikRQM_public
configurationExceptionMessage_GalactikRQM_public(...)
configurationExchange_GalactikRQM_public(...)
configurationConnection_GalactikRQM_public(...)
configurationLogin_GalactikRQM_public(...)
  Implicitly ->configurationRabbitMQ_private(GalactikRQM * info)
start_consuming_public(GalactikRQM * grmq_info);

while(){
  char * data = getCurrentMessage_public();
  String str;
  convertToString(str, data);
  json.parse(str);
  //Real time control
}
*/

void main_rabbitMQ_pthread_private(void * info){
  // Transtypage
  GalactikRQM * _instance = (GalactikRQM *) info;
  // Lancement du coeur de la routine
  rabbitmqConsuming_private(_instance);
  //couverture : code mort
}


void start_consuming_public(GalactikRQM * grmq_info){
  //grmq from Login Procedure
	pthread_t my_consumer_thread;
    // Configuration + Connexion
  configurationRabbitMQ_private(grmq_info);
  //Creation du thread
	pthread_create(&my_consumer_thread, NULL, main_rabbitMQ_pthread_private, (void *) &grmq_info); 
  pthread_join(&my_consumer_thread, NULL);
}



////////////////////////////////////////////////////////
///////////////////////////////////////////////////////



/*
void test_main()
{
    Queue q;
    initializeQueue(&q);

    // Enqueue elements
    enqueue(&q, 10);
    printQueue(&q);

    enqueue(&q, 20);
    printQueue(&q);

    enqueue(&q, 30);
    printQueue(&q);

    // Peek front element
    printf("Front element: %d\n", peek(&q));

    // Dequeue an element
    dequeue(&q);
    printQueue(&q);

    // Peek front element after dequeue
    printf("Front element after dequeue: %d\n", peek(&q));

};*/

