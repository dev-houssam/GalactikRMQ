//
// #include "BackLibGalactikRMQ.c"

#include <iostream>

#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace std;

// Declaration of the C function
extern "C" {
GalactikRQM * _init_GalactikRQM_public();

void configurationExceptionMessage_GalactikRQM_public
	(GalactikRQM * _instance, 
  const char * loginExceptionMessage,
  const char * openingChannelExceptionMessage,
  const char * queueExceptionMessage,
  const char * bindingExceptionMessage,
  const char * consumingExceptionMessage,
  const char * closingChannelExceptionMessage,
  const char * closingConnectionExceptionMessage,
  const char * endingConnectionExceptionMessage);

void configurationExchange_GalactikRQM_public
	(GalactikRQM * _instance, 
  const char * exchange_name,
  const char * bindingKey,
  const char *queuename);


void configurationConnection_GalactikRQM_public
	(GalactikRQM * _instance, const char * hostname, int port);

void configurationLogin_GalactikRQM_public
	(GalactikRQM * _instance, 
  const char * v_host, 
  int channel_max, 
  int frame_max,
  int heartbeat,
  amqp_sasl_method_enum sasl_method,
  const char * username,
  const char * password);

void start_consuming_public
	(GalactikRQM * grmq_info); 

char * getCurrentMessage_public
	(GalactikRQM * _instance);
}


template <typename T>
class GalactikRMQ {
public:
    GalactikRQM * stub;
	
	public:
    void init(){
    	stub = _init_GalactikRQM_public();
    }

    void setExceptionMessage(
		    const String & loginExceptionMessage, 
		    const String & openingChannelExceptionMessage, 
		    const String & queueExceptionMessage, 
		    const String & bindingExceptionMessage, 
		    const String & consumingExceptionMessage, 
		    const String & closingChannelExceptionMessage, 
		    const String & closingConnectionExceptionMessage, 
		    const String & endingConnectionExceptionMessage){
    	configurationExceptionMessage_GalactikRQM_public(stub, 
		    loginExceptionMessage, 
		    openingChannelExceptionMessage, 
		    queueExceptionMessage, 
		    bindingExceptionMessage, 
		    consumingExceptionMessage, 
		    closingChannelExceptionMessage, 
		    closingConnectionExceptionMessage, 
		    endingConnectionExceptionMessage);

    }

    void declareExchange(const String & exchange_name, const String & bindingKey, const String & queuename){
    	configurationExchange_GalactikRQM_public(stub, exchange_name, bindingKey, queuename); 
    }

    void connection(const String & hostname, int port){
    	configurationConnection_GalactikRQM_public(stub, hostname, port);
    }

    void login(const String & , const String & v_host, int channel_max, int frame_max, int heartbeat, const String & username, const String & password){
    	configurationLogin_GalactikRQM_public(stub, v_host, channel_max, frame_max, heartbeat, username, password);
    }


    void start(){
    	start_consuming_public(stub);
    }

    String consumingMessage(){
    	return getCurrentMessage_public(stub);
    }
};




int main(){
	
	GalactikRMQ rabbit();

	rabbit.init();
	rabbit.setExceptionMessage(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
	rabbit.declareExchange(nullptr, nullptr, nullptr);
	rabbit.connection(nullptr, nullptr);
	rabbit.login(nullptr);

	rabbit.start();

	while(true){
		//Consommation
		try{
			auto data_from_Gal =  rabbit.consumingMessage();
			auto data = json::parse(data_from_Gal);
			std::string sensorId = data.value("sensor_id", "unknown");
		    std::string message  = data.value("message", "No message");
		    std::string timestamp= data.value("timestamp", "unknown");
		    std::string severity = data.value("severity", "info");

		    for (auto& notifier : notifiers) {
		        notifier->sendNotification(sensorId, message, timestamp);
		    }
		}catch(const std::exception& e){
			std::cerr << "❌ Erreur parsing JSON : " << e.what() << std::endl;
		}
	}

	return 0;
}







