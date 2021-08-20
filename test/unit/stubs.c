#include "config.h"

#include <time.h>
#include <sys/types.h>
#include <logging_mosq.h>

struct mosquitto_db{

};

int log__printf(struct mosquitto *mosq, unsigned int priority, const char *fmt, ...)
{
	UNUSED(mosq);
	UNUSED(priority);
	UNUSED(fmt);

	return 0;
}

time_t mosquitto_time(void)
{
	return 123;
}

bool net__is_connected(struct mosquitto *mosq)
{
	UNUSED(mosq);
	return false;
}

int net__socket_close(struct mosquitto_db *db, struct mosquitto *mosq)
{
	UNUSED(db);
	UNUSED(mosq);

	return MOSQ_ERR_SUCCESS;
}

int send__pingreq(struct mosquitto *mosq)
{
	UNUSED(mosq);

	return MOSQ_ERR_SUCCESS;
}

void callback__on_disconnect(struct mosquitto *mosq, int rc, const mosquitto_property *props)
{
	UNUSED(mosq);
	UNUSED(rc);
	UNUSED(props);
}

void callback__on_publish(struct mosquitto *mosq, int mid, int reason_code, const mosquitto_property *properties)
{
	UNUSED(mosq);
	UNUSED(mid);
	UNUSED(reason_code);
	UNUSED(properties);
}

void do_client_disconnect(struct mosquitto *mosq, int reason_code, const mosquitto_property *properties)
{
	UNUSED(mosq);
	UNUSED(reason_code);
	UNUSED(properties);
}

int handle__packet(struct mosquitto *context)
{
	UNUSED(context);
	return MOSQ_ERR_SUCCESS;
}

ssize_t net__read(struct mosquitto *mosq, void *buf, size_t count)
{
	UNUSED(mosq);
	UNUSED(buf);
	UNUSED(count);
	return 1;
}

ssize_t net__write(struct mosquitto *mosq, const void *buf, size_t count)
{
	UNUSED(mosq);
	UNUSED(buf);
	UNUSED(count);
	return 1;
}

int send__subscribe(struct mosquitto *mosq, int *mid, int topic_count, char *const *const topic, int topic_qos, const mosquitto_property *properties)
{
	UNUSED(mosq);
	UNUSED(mid);
	UNUSED(topic_count);
	UNUSED(topic);
	UNUSED(topic_qos);
	UNUSED(properties);

	return MOSQ_ERR_SUCCESS;
}

int send__unsubscribe(struct mosquitto *mosq, int *mid, int topic_count, char *const *const topic, const mosquitto_property *properties)
{
	UNUSED(mosq);
	UNUSED(mid);
	UNUSED(topic_count);
	UNUSED(topic);
	UNUSED(properties);

	return MOSQ_ERR_SUCCESS;
}
