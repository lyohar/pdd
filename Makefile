CFLAGS=-g -Wall -Wextra -Werror=implicit-function-declaration
LDFLAGS=-lz -lrt


all: bin/pdd bin/pdd_server


bin/pdd: client.o
	mkdir -p bin
	gcc -o bin/pdd client.o $(LDFLAGS)

bin/pdd_server: server.o
	mkdir -p bin
	gcc -o bin/pdd_server server.o $(LDFLAGS)


client.o: client.c common.h
	gcc -c client.c $(CFLAGS)

server.o: server.c common.h
	gcc -c server.c $(CFLAGS)


clean:
	rm -f bin/pdd bin/pdd_server client.o server.o
	rmdir bin
