#include <stdio.h>
#include <stdlib.h>
#include <zlib.h>
#include <getopt.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "common.h"


#define MAX_SLOT_COUNT 65535


typedef struct args_t {
    char verbose;
    const char *host_name;
    int port_number;
    int slots;
    const char *log_file_name;
    const char *error_log_file_name;
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
    int max_free_slots;
    int free_slots;
    int service_socket_fd;
    client_t client_list;
    client_t term_fd_client;
    int verbose;
} service_t;

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

static int init_sockaddr(struct sockaddr *addr, const char *host, uint16_t port)
{
    if (!host) {
	((struct sockaddr_in*) addr)->sin_family = AF_INET;
	((struct sockaddr_in*) addr)->sin_port = htons(port);
	((struct sockaddr_in*) addr)->sin_addr.s_addr = htonl(INADDR_ANY);
	return 0;
    }

    struct addrinfo hints = {
	.ai_family = AF_INET,
    };
    struct addrinfo *res;
    int ret = getaddrinfo(host, NULL, &hints, &res);
    if (ret) {
	fprintf(stderr, "Failed to resolve address '%s': %s\n", host, gai_strerror(ret));
	return -1;
    }
    if (res) {
	if (res->ai_family == AF_INET) {
	    memcpy(addr, res->ai_addr, res->ai_addrlen);
	    ((struct sockaddr_in*) addr)->sin_port = htons(port);
	    freeaddrinfo(res);
	    return 0;
	} else {
	    fprintf(stderr, "Failed to resolve address '%s': resolved address isn't IPv4 address\n", host);
	}
	freeaddrinfo(res);
    } else {
	fprintf(stderr, "Failed to resolve address '%s'\n", host);
    }
    return -1;
}
static int start_listening(const char *host, uint16_t port)
{
    struct sockaddr_storage addr;
    if (init_sockaddr((struct sockaddr*) &addr, host, port))
	return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd != -1) {
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
	service->max_free_slots = args->slots;
	service->free_slots = args->slots;
	service->service_socket_fd = -1;
	service->term_fd_client.fd = term_fd;
	if ((service->log_fh = fopen(args->log_file_name, "a"))) {
	    if ((service->error_log_fh = fopen(args->error_log_file_name, "a"))) {
		if ((service->service_socket_fd = start_listening(args->host_name, args->port_number)) != -1) {
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
		fprintf(stderr, "Failed to open error log file '%s' for writing: %s\n", args->error_log_file_name, strerror(errno));
	    }
	    fclose(service->log_fh);
	} else {
	    fprintf(stderr, "Failed to open log file '%s' for writing: %s\n", args->log_file_name, strerror(errno));
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
		    memmove(client->buffer, client->buffer + entry_len, client->buffer_off -= entry_len);
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

	    size_t entry_len = sizeof(char) + sizeof(uint32_t);
	    memmove(client->buffer, client->buffer + entry_len, client->buffer_off -= entry_len);

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
		    struct timespec tm;
		    if (!clock_gettime(CLOCK_MONOTONIC, &tm)) {
			timespec_add_msec(&tm, timeout_ms);
			struct itimerspec its = {
			    .it_interval = {
				.tv_sec = 0,
				.tv_nsec = 0,
			    },
			    .it_value = tm,
			};
			timerfd_settime(client->timer_fd, 0, &its, NULL);
			client->slot_requested = 1;
		    } else {
			fprintf(stderr, "Failed to get current time: %s\n", strerror(errno));
			service_drop_client(service, client);
		    }
		}
	    } else {
		fprintf(stderr, "Protocol error: slot already reserved by the client\n");
		service_drop_client(service, client);
	    }
	}
    } break;
    case 'r': {
	size_t entry_len = sizeof(char);
	memmove(client->buffer, client->buffer + entry_len, client->buffer_off -= entry_len);

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

	    size_t entry_len = sizeof(char) + sizeof(uint32_t);
	    memmove(client->buffer, client->buffer + entry_len, client->buffer_off -= entry_len);

	    uint8_t response = 1;
	    if (write(client->fd, &response, sizeof(uint8_t)) != sizeof(uint8_t)) {
		fprintf(stderr, "Failed to write to socket: %s\n", ((errno == EAGAIN) ? "write buffer overflow" : strerror(errno)));
		service_drop_client(service, client);
	    }

	    service_give_free_slots(service, 0);
	}
    } break;
    default: {
	fprintf(stderr, "Protocol error: unhandled command '%c' (%d)\n", op, (int) op);
	service_drop_client(service, client);
    }
    }
}
static int service_main(service_t *service)
{
    fprintf(stderr, "server is ready to accept connections, initial slots: %d\n", (int) service->free_slots);

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
			if (!service_add_client(service, client_sock)) {
			    fprintf(stderr, "Failed to add client: %s\n", strerror(errno));
			    close(client_sock);
			    return 1;
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
    service_t service;
    if (service_init(&service, args, term_fd) == -1)
	return 1;
    int ret = service_main(&service);
    service_uninit(&service);
    return ret;
}

static void print_usage(const char *app)
{
    fprintf(stderr, "Usage: %s -l LOG_FILENAME -e ERROR_LOG_FILENAME -p PORT -a AVAILABLE_SLOTS [-h HOST] [-v]\n", app);
    fprintf(stderr, "\n");
    fprintf(stderr, "  -l LOG_FILENAME          set log filename\n");
    fprintf(stderr, "  -e LOG_ERROR_FILENAME    set error log filename\n");
    fprintf(stderr, "  -p PORT                  set port\n");
    fprintf(stderr, "  -a AVAILABLE_SLOTS       specifies total number of available slots in range [1..%d]\n", (int) MAX_SLOT_COUNT);
    fprintf(stderr, "  -h HOST                  set host\n");
    fprintf(stderr, "  -v                       switch verbose mode\n");
}


static int parse_arguments(args_t *args, int argc, char **argv)
{
    args->slots = 0;
    args->verbose = 0;
    args->port_number = 0;
    args->host_name = NULL;
    args->log_file_name = NULL;
    args->error_log_file_name = NULL;

    int opt;
    while ((opt = getopt(argc, argv, ":l:e:p:h:a:v")) != -1) {
        switch (opt) {
	case 'l':
	    args->log_file_name = optarg;
	    break;
	case 'e':
	    args->error_log_file_name = optarg;
	    break;
	case 'p':
	    args->port_number = atoi(optarg);
	    if (args->port_number <= 0 || args->port_number > 65535) {
		fprintf(stderr, "Invalid service port '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'h':
	    args->host_name = optarg;
	    break;
	case 'a':
	    args->slots = atoi(optarg);
	    if (args->slots <= 0) {
		fprintf(stderr, "Invalid slot count '%s'\n", optarg);
		return -1;
	    }
	    if (args->slots > MAX_SLOT_COUNT) {
		fprintf(stderr, "Invalid slot count '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'v':
	    args->verbose = 1;
	    break;
	default:
            fprintf(stderr, "Unsupported option '%c'\n", opt);
            return -1;
        }
    }

    if (!args->port_number) {
	fprintf(stderr, "Missing mandatory service port option\n");
	return -1;
    }
    if (!args->log_file_name) {
	fprintf(stderr, "Missing mandatory log filename option\n");
	return -1;
    }
    if (!args->error_log_file_name) {
	fprintf(stderr, "Missing mandatory error log filename option\n");
	return -1;
    }
    if (args->slots <= 0) {
	fprintf(stderr, "Missing mandatory slot count option\n");
	return -1;
    }
    if (!args->port_number) {
	fprintf(stderr, "Missing mandatory port number option\n");
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
