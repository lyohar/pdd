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

#include "common.h"


typedef struct args_t {
    const char* host_name;
    size_t port_number;
    ssize_t read_buffer_size;
    size_t decompress_buffer_size;
    size_t slots;
    int zero_slots_max_retries;
    const char* input_file_name;
    int32_t set_max_slot_count;
    int32_t get_max_slot_count;
    char verbose;
} args_t;


static inline int send_all(int sock, const void* data, size_t len)
{
    while (len) {
	ssize_t n = write(sock, data, len);
	if (n <= 0)
	    return -1;
	data += n;
	len -= n;
    }    
    if (len)
        return -1;
    return 0;
}

static inline int recv_all(int sock, void* data, size_t len)
{
    size_t bytes_left = len;
    size_t total_processed = 0;
    ssize_t bytes_processed = 0;
    
    while (bytes_left) {
	bytes_processed = read(sock, data, bytes_left);
        if (!bytes_processed)
            break;
	if (bytes_processed < 0) {
            if (errno != EAGAIN)
                return -1;
        } else {
            total_processed += bytes_processed;
            data += bytes_processed;
            bytes_left -= bytes_processed;
        }
    }    
    if (bytes_left != 0)
        return -1;
    return 0;
}
static int send_log_message(int client_sock, const char *message, uint16_t len, int type)
{
    char op = type;
    uint16_t length = htons(len);
    if (send_all(client_sock, &op, sizeof(char)) ||
	send_all(client_sock, &length, sizeof(uint16_t)) ||
	send_all(client_sock, message, len))
	return -1;
    return 0;
}
static int pdd_connect(args_t args)
{
    int client_sock;
    struct sockaddr_in addr;

    client_sock = socket(AF_INET, SOCK_STREAM, 0);
    if(client_sock < 0) {
        fprintf(stderr, "could not create client socken\n");
        return -1;
    }

    if (inet_pton(AF_INET, args.host_name, &addr.sin_addr) != 1) {
        fprintf(stderr, "could not get address\n");
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(args.port_number);
    if (connect(client_sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        fprintf(stderr, "could not connect to host\n");
        return -1;
    }

    return client_sock;
}	

static int set_max_slot_count(args_t args)
{
    int client_sock = pdd_connect(args);
    if (client_sock < 0)
	return 1;

    char message[5];
    int max_slot_count = htonl(args.set_max_slot_count);
    message[0] = 'S';
    memcpy (message + 1, &max_slot_count, sizeof(int));

    ssize_t ret;
    if ((ret = write(client_sock, message, sizeof(message))) != sizeof(message)) {
        fprintf(stderr, "failed to send configuration request: %s\n", ((ret == -1) ? strerror(errno) : "unexpected kernel TCP buffer overflow"));
        return 1;
    }
    
    uint8_t response;
    if ((ret = read(client_sock, &response, sizeof(uint8_t))) != sizeof(uint8_t)) {
        fprintf(stderr, "failed to send configuration request: %s\n", ((ret == -1) ? strerror(errno) : "connection closed by server"));
        return 1;
    }

    if (!response) {
        fprintf(stderr, "socket count NOT updated (why?)\n");
	return 1;
    }

    fprintf(stderr, "socket count updated\n");
    return 0;
}
static int get_max_slot_count(args_t args)
{
    int client_sock = pdd_connect(args);
    if (client_sock < 0)
	return 1;

    char message[1] = {'G'};
    ssize_t ret;
    if ((ret = write(client_sock, message, sizeof(char))) != sizeof(char)) {
        fprintf(stderr, "failed to send configuration request: %s\n", ((ret == -1) ? strerror(errno) : "unexpected kernel TCP buffer overflow"));
        return 1;
    }
    
    uint32_t response;
    if ((ret = read(client_sock, &response, sizeof(uint32_t))) != sizeof(uint32_t)) {
        fprintf(stderr, "failed to send configuration request: %s\n", ((ret == -1) ? strerror(errno) : "connection closed by server"));
        return 1;
    }
    response = ntohl(response);

    fprintf(stderr, "Maximum slot count at server is %u\n", response);
    return 0;
}

static int decompress_file(args_t args)
{
    //metrics variables
    struct timespec read_duration = {.tv_sec = 0, .tv_nsec = 0};
    struct timespec decomp_duration = {.tv_sec = 0, .tv_nsec = 0};
    struct timespec write_duration = {.tv_sec = 0, .tv_nsec = 0};

    // communication variables
    char op;
    unsigned short length;
    char msg[2048];

    // zero slots metrics
    int zero_slot_retries = 0;
    int zero_slot_reads = 0;
    int total_zero_slot_retries = 0;
    size_t read_count = 0;

    // network variables
    int client_sock = pdd_connect(args);
    if (client_sock < 0)
	return 1;

    FILE* in = fopen(args.input_file_name, "r");
    if (in == NULL) {
        fprintf(stderr, "could not open file for reading\n");
        return 1;
    }

    z_stream s = {0};
    if (inflateInit2(&s, 16+MAX_WBITS) != Z_OK) {
        fprintf(stderr, "could not inflate stream\n");
        return 1;
    }

    void* read_buf = malloc(args.read_buffer_size);
    void* decomp_buf = malloc(args.decompress_buffer_size);

    int res = 0;

    for (;;) {
        size_t cur_zero_slot_read = 0;
        if (res == Z_STREAM_END) {
            fprintf(stderr, "done\n");
            break;
        }

        // check available slot
        op = 'g';
        send_all(client_sock, &op, sizeof(char));
	uint32_t timeout_ms = 1000;
	uint32_t timeout_ms_be = htonl(timeout_ms);
        send_all(client_sock, &timeout_ms_be, sizeof(uint32_t));
	unsigned char response;
        recv_all(client_sock, &response, sizeof(uint8_t));
        length = response;
        //fprintf(stderr, "available slots: %d %d \n", length, bytes_read);
        if (length == 0) {
            // wait and get slot again
            zero_slot_retries += 1;
            total_zero_slot_retries += 1;
            if (zero_slot_retries == args.zero_slots_max_retries) {
                fprintf(stderr, "maximum number of retries exceeded, continue reading anyway\n");
                cur_zero_slot_read = 1;
                zero_slot_reads += 1;
                zero_slot_retries = 0;
		
		length = snprintf(msg, sizeof(msg), "file: %s, maximum retries exceeded, will read anyway", args.input_file_name);
		if (send_log_message(client_sock, msg, length, 'e')) {
		    fprintf(stderr, "Failed to send log message: %s\n", strerror(errno));
		    return 1;
		}
            } else {
                sleep(2);
                continue;
            }
        }

	++read_count;

	{
	    struct timespec begin_time;
	    start_timing(&begin_time);
	    if (!(s.avail_in = fread(read_buf, 1, args.read_buffer_size, in))) {
		length = snprintf(msg, sizeof(msg), "file: %s, unexpected EOF from input file: archive not finished", args.input_file_name);
		if (send_log_message(client_sock, msg, length, 'e'))
		  fprintf(stderr, "Failed to send log message: %s\n", strerror(errno));

		return 1;
	    }
	    finish_timing(&begin_time, &read_duration);
	}

        // reset slot only if we got it
        if (cur_zero_slot_read == 0) {
            op = 'r';
            send_all(client_sock, &op, sizeof(op));
	    recv_all(client_sock, &response, sizeof(uint8_t));
            length = response;
	    if (args.verbose)
		fprintf(stderr, "Sent\n");
        } else {
            length = snprintf(msg, sizeof(msg), "file: %s, finished read by zero slot", args.input_file_name);
	    if (send_log_message(client_sock, msg, length, 'e')) {
		fprintf(stderr, "Failed to send log message: %s\n", strerror(errno));
		return 1;
	    }
        }

        s.next_in = read_buf;
        for (;;) {
            // decompress
	    {
		struct timespec begin_time;
		start_timing(&begin_time);
		s.avail_out = args.decompress_buffer_size;
		s.next_out = decomp_buf;
		res = inflate(&s, Z_NO_FLUSH);
		finish_timing(&begin_time, &decomp_duration);
	    }
            if(res == Z_NEED_DICT || res == Z_DATA_ERROR || res == Z_MEM_ERROR) {
                fprintf(stderr, "decompress failed: %d, %s\n", res, s.msg);
                deflateEnd(&s); free(read_buf); free(decomp_buf); fclose(in);
                return 1;
            }

            // out
	    {
		size_t nbytes = args.decompress_buffer_size - s.avail_out;

		struct timespec begin_time;
		start_timing(&begin_time);
		size_t write_res = fwrite(decomp_buf, 1, nbytes, stdout);
		if (write_res != nbytes) {
		    fprintf(stderr, "could not write to stdout\n");
		    deflateEnd(&s); free(read_buf); free(decomp_buf); fclose(in);
		    return 1;
		}
		finish_timing(&begin_time, &write_duration);
	    }

            if (s.avail_out != 0) {
                break;
            }
        }
    }


    length = snprintf(msg, sizeof(msg), "%s|%lld.%09d|%lld.%09d|%lld.%09d|%d|%d|%d", args.input_file_name,
		      (long long int) read_duration.tv_sec, (int) read_duration.tv_nsec,
		      (long long int) decomp_duration.tv_sec, (int) decomp_duration.tv_nsec,
		      (long long int) write_duration.tv_sec, (int) write_duration.tv_nsec,
		      (int) read_count, (int) zero_slot_reads, (int) total_zero_slot_retries);
    if (send_log_message(client_sock, msg, length, 'l')) {
	fprintf(stderr, "Failed to send log message: %s\n", strerror(errno));
	return 1;
    }

    close(client_sock);
    deflateEnd(&s); free(read_buf); free(decomp_buf); fclose(in);
    return 0;
}

static void print_usage(const char *app)
{
    fprintf(stderr, "Usage: %s -h HOST -p PORT -f INPUT_FNAME -r READ_BUFFER_SIZE -d DECOMPRESS_BUFFER_SIZE -z ZERO_SLOT_MAX_RETIRES [-v]\n", app);
    fprintf(stderr, "       or\n");
    fprintf(stderr, "       %s -h HOST -p PORT -S MAX_FREE_SLOT_COUNT [-v]\n", app);
    fprintf(stderr, "       or\n");
    fprintf(stderr, "       %s -h HOST -p PORT -G [-v]\n", app);
    fprintf(stderr, "\n");
    fprintf(stderr, "  -h HOST                    set host to connect to\n");
    fprintf(stderr, "  -p PORT                    set port to connect to\n");
    fprintf(stderr, "  -f INPUT_FNAME             set input filename\n");
    fprintf(stderr, "  -r READ_BUFFER_SIZE        set read buffer size\n");
    fprintf(stderr, "  -d DECOMPRESS_BUFFER_SIZE  set decompress buffer size\n");
    fprintf(stderr, "  -z ZERO_SLOT_MAX_RETIRES   set error log filename\n");
    fprintf(stderr, "  -v                         turn on verbose mode\n");
}

static int parse_arguments(args_t *args, int argc, char **argv)
{
    args->port_number = 0;
    args->host_name = NULL;
    args->read_buffer_size = 0;
    args->decompress_buffer_size = 0;
    args->input_file_name = NULL;
    args->zero_slots_max_retries = 0;
    args->set_max_slot_count = -1;
    args->get_max_slot_count = -1;
    args->verbose = 0;

    opterr = 0;
    int opt;
    while ((opt = getopt(argc, argv, "h:p:f:r:d:z:S:Gv")) != -1) {
        switch (opt) {
	case 'h':
	    args->host_name = optarg;
	    break;
	case 'p':
	    args->port_number = atoi(optarg);
	    if (args->port_number <= 0 || args->port_number > 65535) {
		fprintf(stderr, "Invalid service port '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'f':
	    args->input_file_name = optarg;
	    break;
	case 'r':
	    if (!(args->read_buffer_size = atoi(optarg))) {
		fprintf(stderr, "Invalid read buffer size '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'd':
	    if (!(args->decompress_buffer_size = atoi(optarg))) {
		fprintf(stderr, "Invalid decompress buffer size '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'z':
	    if ((args->zero_slots_max_retries = atoi(optarg)) < 0) {
		fprintf(stderr, "Invalid zero slots max retires '%s'\n", optarg);
		return -1;
	    }
	    break;
	case 'S':
	    args->set_max_slot_count = atoi(optarg);
	    break;
	case 'G':
	    args->get_max_slot_count = 1;
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

    if (!args->host_name) {
	fprintf(stderr, "Missing mandatory service port option\n");
	return -1;
    }
    if (!args->port_number) {
	fprintf(stderr, "Missing mandatory service port option\n");
	return -1;
    }
    if (args->set_max_slot_count < 0 &&
	args->get_max_slot_count < 0) {
	if (!args->decompress_buffer_size) {
	    fprintf(stderr, "Missing mandatory decompress buffer size option\n");
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
    }
    return 0;
}

int main(int argc, char** argv)
{
    args_t args;
    if (parse_arguments(&args, argc, argv)) {
	print_usage(argv[0]);
	return 1;
    }

    if (args.set_max_slot_count >= 0)
	return set_max_slot_count(args);
    if (args.get_max_slot_count >= 0)
	return get_max_slot_count(args);

    return decompress_file(args);
}
