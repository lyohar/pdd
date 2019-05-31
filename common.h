#ifndef COMMON_H__
#define COMMON_H__

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <zlib.h>
#include <sys/time.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <arpa/inet.h>
#include <string.h>
#include <signal.h>
#include <netdb.h>


static inline void *check_malloc(size_t size)
{
    void *ptr = malloc(size);
    if (!ptr) {
	fprintf(stderr, "Failed to allocate %ld bytes\n", size);
    }
    return ptr;
}

static inline int send_all(int sock, const void *data, size_t len)
{
    while (len) {
	ssize_t n = write(sock, data, len);
	if (n <= 0)
	    return -1;
	data += n;
	len -= n;
    }    
    return 0;
}
static inline int recv_all(int sock, void *data, size_t len)
{
    size_t left = len;
    while (left) {
	ssize_t ret = read(sock, data, left);
        if (!ret)
	    return 0;
        if (ret < 0)
	    return -1;
	data += ret;
	left -= ret;
    }    
    return 1;
}

static int parse_int(const char *str, int min_value, int max_value, int *res)
{
    if (!*str)
	return 0;
    char *eptr;
    long int v = strtol(str, &eptr, 10);
    if (*eptr || v < min_value || v > max_value)
	return 0;
    *res = v;
    return 1;
}
static inline void start_timing(struct timespec *begin_time)
{
    if (clock_gettime(CLOCK_MONOTONIC_RAW, begin_time)) {
	fprintf(stderr, "Failed to get current time: %s\n", strerror(errno));
	exit(1);
    }
}
static inline void finish_timing(const struct timespec *begin_time, struct timespec *duration_r)
{
    struct timespec end_time;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &end_time)) {
	fprintf(stderr, "Failed to get current time: %s\n", strerror(errno));
	exit(1);
    }
    struct timespec duration;
    duration.tv_sec = end_time.tv_sec - begin_time->tv_sec;
    duration.tv_nsec = end_time.tv_nsec - begin_time->tv_nsec;
    if (duration.tv_nsec < 0) {
	duration.tv_sec--;
	duration.tv_nsec += 1000000000;
    }
    duration.tv_sec += duration_r->tv_sec;
    duration.tv_nsec += duration_r->tv_nsec;
    if (duration.tv_nsec >= 1000000000) {
	duration.tv_sec++;
	duration.tv_nsec -= 1000000000;
    }
    *duration_r = duration;
}
static inline int init_sockaddr_v4(struct sockaddr_in *addr, const char *host, uint16_t port)
{
    if (!host) {
	addr->sin_family = AF_INET;
	addr->sin_port = htons(port);
	addr->sin_addr.s_addr = htonl(INADDR_ANY);
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
	    addr->sin_port = htons(port);
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

#endif
