CFLAGS=-W -Wall -I/usr/local/include -g
LDFLAGS=-L/usr/local/lib -g
PROGRAMS= psbv 
LIBRARIES=-lBXP -lpthread -lADTs

all: $(PROGRAMS)

psbv: psbv.o sort.o
	gcc $(LDFLAGS) -o $@ $^ $(LIBRARIES)

psbv.o: psbv.c

