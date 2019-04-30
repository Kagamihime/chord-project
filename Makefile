CC = mpicc
CFLAGS = -Wall -std=c11
COMPILE = $(CC) $(CFLAGS) -c

exo1: exo1.o utils.o
	$(CC) $^ -o $@

exo1.o: src/exo1.c
	$(COMPILE) $< -o $@

init_dht: init_dht.o utils.o
	$(CC) $^ -o $@

init_dht.o: src/init_dht.c
	$(COMPILE) $< -o $@

exo3: exo3.o utils.o
	$(CC) $^ -o $@

exo3.o: src/exo3.c
	$(COMPILE) $< -o $@

skeleton: skeleton.o utils.o
	$(CC) $^ -o $@

skeleton.o: src/skeleton.c
	$(COMPILE) $< -o $@

utils.o: src/utils.c src/utils.h
	$(COMPILE) $< -o $@

clean:
	rm -f *.o exo1 init_dht exo3 skeleton
