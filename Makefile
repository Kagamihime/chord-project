CC = mpicc
CFLAGS = -Wall -std=c11
COMPILE = $(CC) $(CFLAGS) -c

init_dht: init_dht.o utils.o
	$(CC) $^ -o $@

init_dht.o: src/init_dht.c
	$(COMPILE) $< -o $@

skeleton: skeleton.o utils.o
	$(CC) $^ -o $@

skeleton.o: src/skeleton.c
	$(COMPILE) $< -o $@

utils.o: src/utils.c src/utils.h
	$(COMPILE) $< -o $@

clean:
	rm -f *.o init_dht skeleton
