CFLAGS=-g -Wall -Wextra -Werror=implicit-function-declaration
LDFLAGS=-lz -lrt


all: bin/pdd bin/pdd_server


bin/pdd: client.o common.o
	mkdir -p bin
	gcc -o bin/pdd client.o common.o $(LDFLAGS)

bin/pdd_server: server.o common.o
	mkdir -p bin
	gcc -o bin/pdd_server server.o common.o $(LDFLAGS)


common.o: common.c common.h
	gcc -c common.c $(CFLAGS)

client.o: client.c common.h
	gcc -c client.c $(CFLAGS)

server.o: server.c common.h
	gcc -c server.c $(CFLAGS)


clean:
	rm -f bin/pdd bin/pdd_server client.o server.o common.o
	rmdir bin
