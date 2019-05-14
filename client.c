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
    char verbose;
    const char* host_name;
    size_t port_number;
    size_t read_buffer_size;
    size_t decompress_buffer_size;
    size_t slots;
    size_t zero_slots_max_retries;
    const char* input_file_name;
    int32_t set_max_slot_count;
} args_t;


static inline int send_all(int sock, void* data, size_t len) {
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

static inline int recv_all(int sock, void* data, size_t len) {
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

    if (make_socket_non_blocking(client_sock)) {
        fprintf(stderr, "could not make socket non-blocking\n");
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

    if (send_all(client_sock, message, sizeof(message))) {
        fprintf(stderr, "failed to send configuration request\n");
        return 1;
    }

    unsigned char response;
    if (recv_all(client_sock, &response, sizeof(uint8_t))) {
        fprintf(stderr, "failed to send configuration request\n");
        return 1;
    }

    if (!response) {
        fprintf(stderr, "socket count NOT updated (why?)\n");
	return 1;
    }

    fprintf(stderr, "socket count updated\n");
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
    size_t max_log_message_size = 2048;
    char msg[max_log_message_size];

    // zero slots metrics
    size_t zero_slot_retries = 0;
    size_t zero_slot_reads = 0;
    size_t total_zero_slot_retries = 0;

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
		
		// TODO: Log message for this
		fprintf(stderr, "file: %s, maximum retries exceeded, will read anyway%c", args.input_file_name, 0);
			  
		/* { */
		/*   op = 'i'; */
		/*   length = snprintf(msg, max_log_message_size, "file: %s, maximum retries exceeded, will read anyway%c", args.input_file_name, 0); */
		/*   length = htons(length); */
		/*   send_all(client_sock, &op, sizeof(op)); */
		/*   send_all(client_sock, &length, sizeof(length)); */
		/*   send_all(client_sock, msg, ntohs(length)); */
		/* } */
            } else {
                sleep(2);
                continue;
            }
        }

	{
	    struct timespec begin_time;
	    start_timing(&begin_time);
	    s.avail_in = fread(read_buf, 1, args.read_buffer_size, in);
	    finish_timing(&begin_time, &read_duration);
	}

        // reset slot only if we got it
        if (cur_zero_slot_read == 0) {
            op = 'r';
            send_all(client_sock, &op, sizeof(op));
	    recv_all(client_sock, &response, sizeof(uint8_t));
            length = response;
	    fprintf(stderr, "Sent\n");
        } else {
	  fprintf(stderr, "file: %s, finished read by zero slot%c", args.input_file_name, 0);
	  // TODO: Log message for this
	  /* op = 'i'; */
          /*   length = snprintf(msg, max_log_message_size, "file: %s, finished read by zero slot%c", args.input_file_name, 0); */
          /*   length = htons(length); */
          /*   send_all(client_sock, &op, sizeof(op)); */
          /*   send_all(client_sock, &length, sizeof(length)); */
          /*   send_all(client_sock, msg, ntohs(length)); */
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

    op = 'l';
    length = snprintf(msg, max_log_message_size, "%s|%lld.%09d|%lld.%09d|%lld.%09d|%d|%d%c", args.input_file_name,
		      (long long int) read_duration.tv_sec, (int) read_duration.tv_nsec,
		      (long long int) decomp_duration.tv_sec, (int) decomp_duration.tv_nsec,
		      (long long int) write_duration.tv_sec, (int) write_duration.tv_nsec,
		      (int) zero_slot_reads, (int) total_zero_slot_retries, 0);
    length = htons(length);
    send_all(client_sock, &op, sizeof(op));
    send_all(client_sock, &length, sizeof(length));
    send_all(client_sock, msg, ntohs(length));

    close(client_sock);
    deflateEnd(&s); free(read_buf); free(decomp_buf); fclose(in);
    return 0;
}

static void print_usage()
{
    fprintf(stderr, "TODO\n");
}
static int check_arguments()
{
    //TODO
    return 0;
}

int main(int argc, char** argv)
{
    args_t args;
    args.verbose = 0;
    args.port_number = 0;
    args.host_name = NULL;
    args.read_buffer_size = 0;
    args.decompress_buffer_size = 0;
    args.input_file_name = NULL;
    args.zero_slots_max_retries = 0;
    args.set_max_slot_count = -1;

    int opt = 0;
    while ((opt = getopt(argc, argv, ":f:l:r:d:sp:h:a:vz:S:")) != -1) {
        switch (opt) {
	case 'f':
	    args.input_file_name = optarg;
	    break;
	case 'r':
	    args.read_buffer_size = atoi(optarg);
	    break;
	case 'd':
	    args.decompress_buffer_size = atoi(optarg);
	    break;
	case 'p':
	    args.port_number = atoi(optarg);
	    break;
	case 'h':
	    args.host_name = optarg;
	    break;
	case 'v':
	    args.verbose = 1;
	    break;
	case 'z':
	    args.zero_slots_max_retries = atoi(optarg);
	    break;
	case 'S':
	    args.set_max_slot_count = atoi(optarg);
	    break;
	default:
            fprintf(stderr, "Unsupported option '%c'\n", opt);
	    print_usage();
	    return 1;
        }
        if (check_arguments() != 0) {
            fprintf(stderr, "incorrect arguments\n");
            return 1;
        }
    }

    if (args.set_max_slot_count >= 0)
	return set_max_slot_count(args);

    return decompress_file(args);
}
