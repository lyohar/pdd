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

#endif
