#ifndef UTILS_H
#define UTILS_H

#define M 10 // DHT constant
#define NB_PEERS 6 // Number of peers in the system

#define TAGINIT 0

/* The identifier of a peer for exo1 */
typedef struct id {
    int id;     /* the MPI id */
    int hash;   /* the CHORD id */
} ID;

int f(int n); // Hash function

void simulator(void);

int cmpfunc(const void *a, const void *b);

#endif
