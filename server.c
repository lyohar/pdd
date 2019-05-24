#include <stdio.h>
#include <stdlib.h>
#include <zlib.h>
#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "common.h"


/* 1 billion slots: */
#define MAX_SLOT_COUNT 1000000000


typedef struct args_t {
    char socket_reuse_addr;
    char verbose;
    const char *host;
    int port;
    int max_client_connections;
    int slots;
    const char *log_filename;
    const char *error_log_filename;
    int term_fd;
} args_t;

#define CLIENT_MESSAGE_SIZE 2048
#define CLIENT_BUFFER_SIZE 2176
typedef struct client_t {
    int fd;
    int timer_fd;
    char slot_requested;
    char slot_reserved;
    char buffer[CLIENT_BUFFER_SIZE];
    size_t buffer_off;
    struct client_t *prev;
    struct client_t *next;
} client_t;


typedef struct service_t {
    int epoll_fd;
    FILE *log_fh;
    FILE *error_log_fh;
    int max_client_connections;
    int client_connection_count;
    int max_free_slots;
    int free_slots;
    int service_socket_fd;
    client_t client_list;
    client_t term_fd_client;
    int verbose;
} service_t;

static inline int make_socket_non_blocking (int fd)
{
    int flags = fcntl (fd, F_GETFL, 0);
    if (flags == -1) {
        fprintf(stderr, "Failed to get socket flags: %s\n", strerror(errno));
        return -1;
    }
    if (fcntl (fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        fprintf(stderr, "Failed to set socket flags: %s\n", strerror(errno));
        return -1;
    }
    return 0;
}
static inline void timespec_add_msec(struct timespec *tm, int msec)
{
    tm->tv_sec += msec/1000;
    tm->tv_nsec += msec%1000*1000000;
    if (tm->tv_nsec >= 1000000000) {
	tm->tv_sec++;
	tm->tv_nsec -= 1000000000;
    }
}
static void init_client_list(client_t *client_list, int service_fd)
{
    client_list->fd = service_fd;
    client_list->slot_reserved = 0;
    client_list->buffer_off = 0;
    client_list->next = client_list;
    client_list->prev = client_list;
}
static client_t *service_add_client(service_t *service, int fd)
{
    if (make_socket_non_blocking(fd) == -1)
	return NULL;
    client_t *client = malloc(sizeof(client_t));
    if (client) {
	struct epoll_event event;
	event.data.ptr = client;
	event.events = EPOLLIN;
	if (epoll_ctl(service->epoll_fd, EPOLL_CTL_ADD, fd, &event) != -1) {
	    client->fd = fd;
	    if ((client->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK)) != -1) {
		event.data.ptr = ((void *) client) + 1;
		event.events = EPOLLIN;
		if (epoll_ctl(service->epoll_fd, EPOLL_CTL_ADD, client->timer_fd, &event) != -1) {
		    client->slot_reserved = 0;
		    client->slot_requested = 0;
		    client->buffer_off = 0;
		    client->prev = service->client_list.prev;
		    service->client_list.prev->next = client;
		    service->client_list.prev = client;
		    client->next = &service->client_list;
		    service->client_connection_count++;
		    return client;
		} else {
		    fprintf(stderr, "Failed to add client to epoll: %s\n", strerror(errno));
		}
		close(client->timer_fd);
	    } else {
		fprintf(stderr, "Failed to create timer FD: %s\n", strerror(errno));
	    }
	    epoll_ctl(service->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
	} else {
	    fprintf(stderr, "Failed to add client to epoll: %s\n", strerror(errno));
	}
	free(client);
    }
    return NULL;
}
static int service_reserve_slot(service_t *service, client_t *client)
{
    if (client->slot_reserved || (service->free_slots <= 0))
	return 0;

    client->slot_reserved = 1;
    service->free_slots--;
    return 1;
}
static int service_release_slot(service_t *service, client_t *client)
{
    if (!client->slot_reserved)
	return 0;

    client->slot_reserved = 0;
    service->free_slots++;
    return 1;
}
static void service_drop_client(service_t *service, client_t *client)
{
    if (service_release_slot(service, client)) {
	if (service->verbose)
	    fprintf(stderr, "Slot released (%d slots free now)\n", service->free_slots);
    }
    if (epoll_ctl(service->epoll_fd, EPOLL_CTL_DEL, client->fd, NULL))
	fprintf(stderr, "Failed to delete entry %d from epoll structure: %s\n", client->fd, strerror(errno));
    if (epoll_ctl(service->epoll_fd, EPOLL_CTL_DEL, client->timer_fd, NULL))
	fprintf(stderr, "Failed to delete entry %d from epoll structure: %s\n", client->timer_fd, strerror(errno));
    close(client->fd);
    close(client->timer_fd);
    client->prev->next = client->next;
    client->next->prev = client->prev;
    free(client);
    service->client_connection_count--;
}
static void service_clear_clients(service_t *service)
{
    while (service->client_list.next != &service->client_list)
	service_drop_client(service, service->client_list.next);
}
/*
  returns:
  1 on new data
  0 on absence of new data (false alarm)
  -1 on termination
*/
static int client_read_to_buffer(client_t *client)
{
    ssize_t n = read(client->fd, client->buffer + client->buffer_off, CLIENT_BUFFER_SIZE - client->buffer_off);
    if (n > 0) {
	client->buffer_off += n;
	return 1;
    }
    if (n < 0) {
	if (errno == EAGAIN)
	    return 0;
	fprintf(stderr, "Failed to read from client: %s\n", strerror(errno));
	return -1;
    }
    return -1;
}
static inline void client_remove_messaage(client_t *client, size_t message_len)
{
    memmove(client->buffer, client->buffer + message_len, client->buffer_off -= message_len);
}

static int start_listening(const char *host, uint16_t port, int reuse_addr)
{
    struct sockaddr_in addr;
    if (init_sockaddr_v4(&addr, host, port))
	return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd != -1) {
	if (reuse_addr) {
	    int on_flag = 1;
	    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on_flag, sizeof(int)) < 0)
		fprintf(stderr, "WARNING: Failed to set socket into address reusing mode: %s\n", strerror(errno));
	}
	if (!bind(fd, (struct sockaddr*) &addr, sizeof(addr))) {
	    if (make_socket_non_blocking(fd) != -1) {
		if (!listen(fd, 1024)) {
		    fprintf(stderr, "Listening at '%s:%d'\n", (host ? host : "0.0.0.0"), (int) port);
		    return fd;
		} else {
		    fprintf(stderr, "Failed to start listening: %s\n", strerror(errno));
		}
	    } else {
		fprintf(stderr, "Failed to make socken non blocking: %s\n", strerror(errno));
	    }
	} else {
	    fprintf(stderr, "Failed to bind server socket to address: %s\n", strerror(errno));
	}
	close(fd);
    } else {
        fprintf(stderr, "Failed to create server socket: %s\n", strerror(errno));
    }
    return -1;
}
static int service_init(service_t *service, const args_t *args, int term_fd)
{
    if ((service->epoll_fd = epoll_create(1)) != -1) {
	service->max_client_connections = args->max_client_connections;
	service->client_connection_count = 0;
	service->max_free_slots = args->slots;
	service->free_slots = args->slots;
	service->service_socket_fd = -1;
	service->term_fd_client.fd = term_fd;
	if ((service->log_fh = fopen(args->log_filename, "a"))) {
	    if ((service->error_log_fh = fopen(args->error_log_filename, "a"))) {
		if ((service->service_socket_fd = start_listening(args->host, args->port, args->socket_reuse_addr)) != -1) {
		    struct epoll_event event;
		    event.data.ptr = &service->client_list;
		    event.events = EPOLLIN;
		    if (epoll_ctl(service->epoll_fd, EPOLL_CTL_ADD, service->service_socket_fd, &event) != -1) {
			event.data.ptr = &service->term_fd_client;
			event.events = EPOLLIN;
			if (epoll_ctl(service->epoll_fd, EPOLL_CTL_ADD, service->term_fd_client.fd, &event) != -1) {
			    init_client_list(&service->client_list, service->service_socket_fd);
			    service->verbose = args->verbose;
			    return 0;
			} else {
			    fprintf(stderr, "Failed to add termination event FD to epoll: %s\n", strerror(errno));
			}
		    } else {
			fprintf(stderr, "Failed to add server socket to epoll: %s\n", strerror(errno));
		    }
		}
		fclose(service->error_log_fh);
	    } else {
		fprintf(stderr, "Failed to open error log file '%s' for writing: %s\n", args->error_log_filename, strerror(errno));
	    }
	    fclose(service->log_fh);
	} else {
	    fprintf(stderr, "Failed to open log file '%s' for writing: %s\n", args->log_filename, strerror(errno));
	}
	close(service->epoll_fd);
    } else {
        fprintf(stderr, "Failed to create epoll structure: %s\n", strerror(errno));
    }
    return -1;
}
static void service_uninit(service_t *service)
{
    service_clear_clients(service);
    close(service->epoll_fd);
    close(service->service_socket_fd);
    fclose(service->log_fh);
    fclose(service->error_log_fh);
}
static inline void service_log(service_t *service, const char *message, size_t len)
{
    struct timespec now;
    struct tm now_tm;
    char buffer[256];
    *buffer = '\0';
    if (!clock_gettime(CLOCK_REALTIME, &now) && gmtime_r(&now.tv_sec, &now_tm))
	buffer[strftime(buffer, sizeof(buffer) - 1, "%F %T", &now_tm)] = '\0';
    fprintf(service->log_fh, "%s.%09d|%.*s\n", buffer, (int) now.tv_nsec, (int) len, message);
    fflush(service->log_fh);
}
static inline void service_log_error(service_t *service, const char *message, size_t len)
{
    struct timespec now;
    struct tm now_tm;
    char buffer[256];
    *buffer = '\0';
    if (!clock_gettime(CLOCK_REALTIME, &now) && gmtime_r(&now.tv_sec, &now_tm))
	buffer[strftime(buffer, sizeof(buffer) - 1, "%F %T", &now_tm)] = '\0';
    fprintf(service->error_log_fh, "%s.%09d|%.*s\n", buffer, (int) now.tv_nsec, (int) len, message);
    fflush(service->error_log_fh);
}
static void service_give_free_slots(service_t *service, int one)
{
    client_t *client = service->client_list.next;
    while ((service->free_slots > 0) && (client != &service->client_list)) {
	if (client->slot_requested) {
	    client->slot_requested = 0;
	    service_reserve_slot(service, client);
	    if (service->verbose)
		fprintf(stderr, "Slot reserved (%d slots free now)\n", service->free_slots);
	    uint8_t response = 1;
	    struct itimerspec its = {
		.it_interval = {
		    .tv_sec = 0,
		    .tv_nsec = 0,
		},
		.it_value = {
		    .tv_sec = ((sizeof(time_t) == 8) ? INT64_MAX : INT32_MAX),
		    .tv_nsec = 0,
		},
	    };
	    timerfd_settime(client->timer_fd, 0, &its, NULL);
	    if (write(client->fd, &response, sizeof(uint8_t)) == sizeof(uint8_t)) {
		if (one)
		    break;
	    } else {
		fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
		service_drop_client(service, client);
	    }
	}
	client = client->next;
    }
}
static void service_handle_input(service_t *service, client_t *client)
{
    if (!client->buffer_off)
	return;

    char op = client->buffer[0];
    switch (op) {
    case 'l':
    case 'e': {
	if (client->buffer_off >= (sizeof(char) + sizeof(uint16_t))) {
	    uint32_t len;
	    memcpy(&len, client->buffer + sizeof(char), sizeof(uint16_t));
	    len = ntohs(len);
	    if (len <= CLIENT_MESSAGE_SIZE) {
		size_t entry_len = sizeof(char) + sizeof(uint16_t) + len;
		if (client->buffer_off >= entry_len) {
		    if (op == 'l')
			service_log(service, client->buffer + (sizeof(char) + sizeof(uint16_t)), len);
		    else
			service_log_error(service, client->buffer + (sizeof(char) + sizeof(uint16_t)), len);
		    client_remove_messaage(client, entry_len);
		    uint8_t response = 0;
		    if (write(client->fd, &response, sizeof(uint8_t)) != sizeof(uint8_t)) {
			fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
			service_drop_client(service, client);
		    }
		}
	    } else {
		if (op == 'l')
		    fprintf(stderr, "Protocol error: too big log message\n");
		else
		    fprintf(stderr, "Protocol error: too big error log message\n");
		service_drop_client(service, client);
	    }
	}
    } break;
    case 'g': {
	if (client->buffer_off >= (sizeof(char) + sizeof(uint32_t))) {
	    uint32_t timeout_ms;
	    memcpy(&timeout_ms, client->buffer + sizeof(char), sizeof(uint32_t));
	    timeout_ms = ntohl(timeout_ms);

	    client_remove_messaage(client, sizeof(char) + sizeof(uint32_t));

	    if (!client->slot_reserved) {
		if ((service->free_slots > 0) || !timeout_ms) {
		    /* Immediate answer */
		    uint8_t response = (service->free_slots > 0) ? 1 : 0;
		    if (write(client->fd, &response, sizeof(uint8_t)) == sizeof(uint8_t)) {
			if (service->free_slots > 0)
			    service_reserve_slot(service, client);
			if (service->verbose) {
			    if (client->slot_reserved)
				fprintf(stderr, "Given slot (%d slots free now)\n", service->free_slots);
			    else
				fprintf(stderr, "Declined slot (%d slots free now)\n", service->free_slots);
			}
		    } else {
			fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
			service_drop_client(service, client);
		    }
		} else {
		    /* Arming timeout */
		    if (service->verbose)
			fprintf(stderr, "Arming declining slot on timeout\n");
		    struct itimerspec its = {
			.it_interval = {
			    .tv_sec = 0,
			    .tv_nsec = 0,
			},
			.it_value = {
			    .tv_sec = timeout_ms/1000,
			    .tv_nsec = timeout_ms%1000*1000000,
			}
		    };
		    timerfd_settime(client->timer_fd, 0, &its, NULL);
		    client->slot_requested = 1;
		}
	    } else {
		fprintf(stderr, "Protocol error: slot already reserved by the client\n");
		service_drop_client(service, client);
	    }
	}
    } break;
    case 'r': {
	client_remove_messaage(client, sizeof(char));

	int previous_free_slots = service->free_slots;
	int drop_client = 0;
	if (client->slot_reserved) {
	    if (service->verbose)
		fprintf(stderr, "Releasing slot (%d slot free now)...\n", service->free_slots);
	    service_release_slot(service, client);
	    if (service->verbose)
		fprintf(stderr, "1; Slot released (%d slot free now)\n", service->free_slots);
	    uint8_t response = 1;
	    if (write(client->fd, &response, sizeof(uint8_t)) != sizeof(uint8_t)) {
		fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
		drop_client = 1;
	    }
	} else {
	    fprintf(stderr, "Protocol error: requested to release unreserved slot\n");
	    drop_client = 1;
	}
	if (drop_client)
	    service_drop_client(service, client);
	if ((previous_free_slots <= 0) && (service->free_slots > 0))
	    service_give_free_slots(service, 1);
    } break;
    case 'S': {
	if (client->buffer_off >= (sizeof(char) + sizeof(uint32_t))) {
	    uint32_t new_max_free_slots_unsigned;
	    memcpy(&new_max_free_slots_unsigned, client->buffer + sizeof(char), sizeof(uint32_t));
	    new_max_free_slots_unsigned = ntohl(new_max_free_slots_unsigned);
	    if (new_max_free_slots_unsigned > MAX_SLOT_COUNT)
		new_max_free_slots_unsigned = MAX_SLOT_COUNT;
	    int new_max_free_slots = new_max_free_slots_unsigned;
	    service->free_slots += new_max_free_slots - service->max_free_slots;
	    service->max_free_slots = new_max_free_slots;

	    client_remove_messaage(client, sizeof(char) + sizeof(uint32_t));

	    uint8_t response = 1;
	    if (write(client->fd, &response, sizeof(uint8_t)) != sizeof(uint8_t)) {
		fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
		service_drop_client(service, client);
	    }

	    service_give_free_slots(service, 0);
	    fprintf(stderr, "New max free slots: %d\n", (int) service->max_free_slots);
	}
    } break;
    case 'G': {
	client_remove_messaage(client, sizeof(char));

	uint32_t response = htonl(service->max_free_slots);
	if (write(client->fd, &response, sizeof(uint32_t)) != sizeof(uint32_t)) {
	    fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
	    service_drop_client(service, client);
	}

	service_give_free_slots(service, 0);
    } break;
    default: {
	fprintf(stderr, "Protocol error: unhandled command '%c' (%d)\n", op, (int) op);
	service_drop_client(service, client);
    }
    }
}
static int service_main(service_t *service)
{
    fprintf(stderr, "Server is ready to accept connections, max free slots: %d\n", (int) service->max_free_slots);

    struct epoll_event events[256];
    int events_num;
    while ((events_num = epoll_wait(service->epoll_fd, events, sizeof(events)/sizeof(events[0]), -1)) >= 0 || errno == EINTR) {
	int i;
        for (i = 0; i < events_num; i++) {
	    client_t *client = events[i].data.ptr;
	    int is_timerfd = (uintptr_t) client & 1;
	    client = (client_t*) (((uintptr_t) client) & ~1ULL);
	    if (is_timerfd) {
		uint64_t count;
		if ((read(client->timer_fd, &count, sizeof(uint64_t)) == sizeof(uint64_t)) && count) {
		    if (service->verbose)
			fprintf(stderr, "Slot request timed out\n");
		    if (client->slot_requested) {
			client->slot_requested = 0;
			uint8_t response = 0;
			if (write(client->fd, &response, sizeof(uint8_t)) != sizeof(uint8_t)) {
			    fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
			    service_drop_client(service, client);
			}
		    }
		}
	    } else {
		if (client->fd == service->term_fd_client.fd)
		    return 0;
		if (client->fd == service->service_socket_fd) {
		    int client_sock = accept(service->service_socket_fd, NULL, NULL);
		    if (client_sock >= 0) {
			if (service->client_connection_count < service->max_client_connections) {
			    if (!service_add_client(service, client_sock)) {
				fprintf(stderr, "Failed to add client: %s\n", strerror(errno));
				close(client_sock);
				return 1;
			    }
			} else {
			    fprintf(stderr, "Failed to accept new client: exceeding configured maximum connection count\n");
			    close(client_sock);
			}
		    } else {
			if (errno != EAGAIN) {
			    fprintf(stderr, "Failed to accept client socket\n");
			    return 1;
			}
		    }
		} else {
		    int status = client_read_to_buffer(client);
		    if (status > 0) {
			service_handle_input(service, client);
		    } else if (status < 0) {
			service_drop_client(service, client);
		    }
		}
	    }
        }
    }
    return 0;
}
static int run_server(const args_t *args, int term_fd)
{
    if (args->verbose) {
	fprintf(stderr, "Starting service with configuration:\n");
	fprintf(stderr, "                endpoint = '%s:%d'\n", args->host ? args->host : "0.0.0.0", (int) args->port);
	fprintf(stderr, "            log filename = '%s'\n", args->log_filename);
	fprintf(stderr, "      error log filename = '%s'\n", args->error_log_filename);
	fprintf(stderr, "  max client connections = %d\n", args->max_client_connections);
	fprintf(stderr, "     max available slots = %d\n", args->slots);
	fprintf(stderr, "                 verbose = on\n");
	fprintf(stderr, "\n");
    }
    service_t service;
    if (service_init(&service, args, term_fd) == -1)
	return 1;
    int ret = service_main(&service);
    service_uninit(&service);
    return ret;
}

static void print_usage(const char *app)
{
    fprintf(stderr, "Usage: %s -p PORT -l LOG_FILENAME -e ERROR_LOG_FILENAME -m MAX_CLIENT_CONNECTIONS -a AVAILABLE_SLOTS [-h HOST] [-r] [-v]\n", app);
    fprintf(stderr, "\n");
    fprintf(stderr, "  -p PORT                      set port\n");
    fprintf(stderr, "  -l LOG_FILENAME              set log filename\n");
    fprintf(stderr, "  -e LOG_ERROR_FILENAME        set error log filename\n");
    fprintf(stderr, "  -m MAX_CLIENT_CONNECTIONS    set max incoming client connections\n");
    fprintf(stderr, "  -a AVAILABLE_SLOTS           specifies total number of available slots in range [1..%d]\n", (int) MAX_SLOT_COUNT);
    fprintf(stderr, "  -h HOST                      set host\n");
    fprintf(stderr, "  -r                           put listening socket into address reuse mode\n");
    fprintf(stderr, "  -v                           turn on verbose mode\n");
}


static int parse_arguments(args_t *args, int argc, char **argv)
{
    args->socket_reuse_addr = 0;
    args->verbose = 0;
    args->port = 0;
    args->host = NULL;
    args->max_client_connections = -1;
    args->slots = -1;
    args->log_filename = NULL;
    args->error_log_filename = NULL;

    opterr = 0;
    int int_value;
    int opt;
    while ((opt = getopt(argc, argv, "l:e:p:h:m:a:rv")) != -1) {
        switch (opt) {
	case 'l':
	    args->log_filename = optarg;
	    break;
	case 'e':
	    args->error_log_filename = optarg;
	    break;
	case 'p':
	    if (!parse_int(optarg, 1, 65535, &int_value)) {
		fprintf(stderr, "Invalid service port '%s'\n", optarg);
		return -1;
	    }
	    args->port = int_value;
	    break;
	case 'h':
	    args->host = optarg;
	    break;
	case 'm':
	    if (!parse_int(optarg, 0, 1000000000 /* 1 billion connections */, &args->max_client_connections)) {
		fprintf(stderr, "Invalid max incomming connection count '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'a':
	    if (!parse_int(optarg, 0, MAX_SLOT_COUNT, &args->slots)) {
		fprintf(stderr, "Invalid slot count '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'r':
	    args->socket_reuse_addr = 1;
	    break;
	case 'v':
	    args->verbose = 1;
	    break;
	case '?':
	case ':':
            fprintf(stderr, "Invalid arguments\n");
            return -1;
	default:
            fprintf(stderr, "Unsupported option '%c'\n", opt);
            return -1;
        }
    }

    if (!args->port) {
	fprintf(stderr, "Missing mandatory service port option\n");
	return -1;
    }
    if (!args->log_filename) {
	fprintf(stderr, "Missing mandatory log filename option\n");
	return -1;
    }
    if (!args->error_log_filename) {
	fprintf(stderr, "Missing mandatory error log filename option\n");
	return -1;
    }
    if (args->max_client_connections < 0) {
	fprintf(stderr, "Missing mandatory max incomming connections option\n");
	return -1;
    }
    if (args->slots < 0) {
	fprintf(stderr, "Missing mandatory slot count option\n");
	return -1;
    }
    return 0;
}

static int term_fd = -1;

static void sigpipe_handler(int signum, siginfo_t *siginfo, void *ud)
{
    (void) signum;
    (void) siginfo;
    (void) ud;
    /* NOOP */
}
static void term_handler(int signum, siginfo_t *siginfo, void *ud)
{
    (void) signum;
    (void) siginfo;
    (void) ud;

    if (term_fd == -1) {
	fprintf(stderr, "Dirty exit: failed to create event FD for clean termination\n");
	exit(1);
    }
    eventfd_write(term_fd, 1);
}


#define ADD_SIGACTION(SIGNAME,handler) do {                             \
	struct sigaction sa = {};                                       \
	sa.sa_sigaction = handler;                                      \
	sigaction (SIGNAME, (const struct sigaction *) &sa, (struct sigaction *) NULL); \
    } while (0)

int main(int argc, char **argv)
{
    term_fd = eventfd(0, 0);
    ADD_SIGACTION(SIGPIPE, sigpipe_handler);
    ADD_SIGACTION(SIGTERM, term_handler);
    ADD_SIGACTION(SIGINT, term_handler);

    args_t args;
    if (parse_arguments(&args, argc, argv)) {
	print_usage(argv[0]);
	return 1;
    }

    int ret = run_server(&args, term_fd);
    close(term_fd);
    return ret;
}
