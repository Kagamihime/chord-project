#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <mpi.h>

#include "utils.h"

/* TAG TYPES*/
#define FORWARD 0
#define SEARCH 1
#define RESULT 2
#define INIT 3
#define INITIATOR 4
#define FINISHED 5

/* the maximum of keys one site can have */
#define MAX_CAPACITY 10

/* NODE SPECIFIC VARIABLES */
int p; /* rank of the site */
int id_p; /*chord identifier f(p) */
ID finger_p[NB_PEERS]; /* finger table of p */
ID succ_p;  /* the node's successor */
int keys_p[MAX_CAPACITY]; /* the keys that belong to node p : note that here we don't mind about the keys */

/* FOR THE MASTER SITE */
int nodes[NB_PEERS];

// The function that finds which node is responsible
// for a value
// Is used only by the master process in order
// to calculate the finger tables
int find_responsible(int val)
{
    if (val >= (1 << M)) {
        val = val % (1 << M);
    }

    for (int i = 0; i < NB_PEERS; i++) {
        if (val <= nodes[i]) {
            return i;
        }
    }

    return 0;
}

// The function that initializes the graph
// by calculating the finger table of each process
// and sending it to them
void simulateur(void)
{
    int max = (1 << M) - 1;

    for (int i = 0; i < NB_PEERS; i++) {
        nodes[i] = rand() % max;
    }

    qsort(nodes, NB_PEERS, sizeof(int), cmpfunc);

    for (int i = 0; i < NB_PEERS; i++) {
        printf("noeud %d : %d\n", i + 1, nodes[i]);
    }

    for (int i = 0; i < NB_PEERS; i++) {
        int rank = nodes[i];

        MPI_Send(&rank, 1, MPI_INT, i + 1, INIT, MPI_COMM_WORLD);

        for (int j = 0; j < NB_PEERS; j++) {
            int val = (nodes[i] + (1 << j)) % (1 << M);
            int respo = find_responsible(val) + 1;

            MPI_Send(&respo, 1, MPI_INT, i + 1, INIT, MPI_COMM_WORLD);
            MPI_Send(&nodes[respo - 1], 1, MPI_INT, i + 1, INIT, MPI_COMM_WORLD);
        }
    }
}

// The function used by the nodes to received the values
// of their finger tables that the master process
// has sent them
void rcv_finger()
{
    MPI_Status status;

    for (int i = 0; i < NB_PEERS; i++) {
        int val = 0, hash = 0;

        MPI_Recv(&val, 1, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);
        finger_p[i].id = val;

        MPI_Recv(&hash, 1, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);
        finger_p[i].hash = hash;
    }

    succ_p = finger_p[0];
}

void show_finger()
{
    printf("I AM NODE %d this is my finger table\n", p);

    for (int i = 0; i < NB_PEERS; i++) {
        printf("%d : %d contact %d\n", i, finger_p[i].hash, finger_p[i].id);
    }
}

// The function that returns the ID j such as the hash belongs in [j , id_p[
// If there is no result from the finger table,
// an ID {-1, -1} is returned to signal that we must
// use the successor
ID find_next(int hash)
{
    int K = 1 << M;
    ID err = {
        .hash = -1,
        .id = -1,
    };

    for (int i = NB_PEERS - 1; i >= 0; i--) {
        if (finger_p[i].hash < id_p) {
            if (hash >= finger_p[i].hash && hash < id_p) {
                return finger_p[i];
            }
        } else {
            if ((hash >= finger_p[i].hash && hash < K - 1) || (hash >= 0 && hash <= id_p)) {
                return finger_p[i];
            }
        }
    }

    return err;
}

// The function that needs to be implemented if one wishes
// use this program and store keys
int contains_key(int hash)
{
    return 1;
}


// Function that returns true if the process rank
// can be reached by p
int contains_finger(int rank)
{
    for (int i = 0; i < NB_PEERS; i++) {
        if (finger_p[i].id == rank) {
            return 1;
        }
    }

    return 0;
}

void receive()
{
    MPI_Status status;

    for (;;) {
        int hash, caller, result, holder;
        ID dest;

        MPI_Recv(&hash, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        switch (status.MPI_TAG) {
            case FORWARD:
                // Message received if a process forwarded to p the search of the key

                MPI_Recv(&caller, 1, MPI_INT, status.MPI_SOURCE, FORWARD, MPI_COMM_WORLD, &status);

                // it must find the next process to send it to

                ID target = find_next(hash);

                printf("Process (%d, %d) received a FORWARD.\n", p, id_p);

                if (status.MPI_SOURCE == p) {
                    printf("problem\n");

                    MPI_Send(&id_p, 1, MPI_INT, 0, FINISHED, MPI_COMM_WORLD);
                }

                if (target.hash == -1) {
                    // If the key isn't included in any of the finger table's intervals
                    // the successor is in charge of the key, we must warn it

                    printf("sending SEARCH from (%d,%d) to  SUCCESSOR (%d,%d)\n", p, id_p, succ_p.id, succ_p.hash);

                    MPI_Send(&hash, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                } else {
                    // else juste forward the request to the process found earlier

                    printf("sending FORWARD from (%d,%d) to (%d,%d)\n", p, id_p, target.id, target.hash);

                    MPI_Send(&hash, 1, MPI_INT, target.id, FORWARD, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, target.id, FORWARD, MPI_COMM_WORLD);
                }

                break;

            case SEARCH:
                // Message received if the process is in charge of the key we are looking for

                MPI_Recv(&caller, 1, MPI_INT, status.MPI_SOURCE, SEARCH, MPI_COMM_WORLD, &status);
                result = contains_key(hash);

                if (caller != id_p) {
                    // If we are not the initiator of the request, send RESULT back to the caller
                    // The caller is reached as usual with find_next

                    ID target = find_next(caller);

                    printf("process (%d, %d) contains the key.\n", p, id_p);
                    printf("sending RESULT from (%d,%d) to (%d,%d)\n", p, id_p, target.id, target.hash);

                    MPI_Send(&hash, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&id_p, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&result, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                } else {
                    // If we initiated the search, we can warn the master the process that the algorithm is over

                    printf("sending FINISHED from (%d,%d) to (0)\n", p, id_p);

                    MPI_Send(&id_p, 1, MPI_INT, 0, FINISHED, MPI_COMM_WORLD);
                }

                break;

            case RESULT:
                MPI_Recv(&caller, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);
                MPI_Recv(&holder, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);
                MPI_Recv(&result, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);

                // Message received when the result of the search is trying
                // to come back to the caller

                if (caller == id_p) {
                    // if we happen to be the caller, the search is over

                    printf("sending FINISHED from (%d,%d) to (0)\n", p, id_p);

                    MPI_Send(&holder, 1, MPI_INT, 0, FINISHED, MPI_COMM_WORLD);

                } else {
                    // Else we must forward the result in the usual manner

                    ID target = find_next(caller);

                    if (target.hash == id_p) {
                        MPI_Send(&holder, 1, MPI_INT, 0, FINISHED, MPI_COMM_WORLD);
                    }

                    printf("sending RESULT from (%d,%d) to (%d,%d)\n", p, id_p, target.id, target.hash);

                    MPI_Send(&hash, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&holder, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&result, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                }

                break;

            case INITIATOR:
                // Message received by the process that must initiate the algorithm
                //

                // Searches to whom the next message must be sent
                dest = find_next(hash);

                if (dest.hash  == -1) {
                    // the successor is in charge of the key we are looking for

                    printf("sending SEARCH from (%d,%d) to (%d,%d)\n", p, id_p, dest.id, dest.hash);

                    MPI_Send(&hash, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                    MPI_Send(&id_p, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                } else {
                    // just forwarding the request to the next target

                    printf("sending FORWARD from (%d,%d) to (%d,%d)\n", p, id_p, dest.id, dest.hash);

                    MPI_Send(&hash, 1, MPI_INT, dest.id, FORWARD, MPI_COMM_WORLD);
                    MPI_Send(&id_p, 1, MPI_INT, dest.id, FORWARD, MPI_COMM_WORLD);
                }

                break;

            case FINISHED:
                // Message sent by the master process when it has received the result
                // Every process receives this message once everyting is over

                return;
            default:
                printf("process %d has received an unknown message \n", p);

                break;
        }
    }
}



int main(int argc, char *argv[])
{
    int nb_proc;
    MPI_Status status;

    srand(time(NULL));

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nb_proc);

    if (nb_proc != NB_PEERS + 1) {
        fprintf(stderr, "Incorrect number of processes!\n");
        fprintf(stderr, "There should be %d\n", NB_PEERS + 1);
        goto failure;
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &p);

    if (p == 0) {
        simulateur();
    } else {
        MPI_Recv(&id_p, 1, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);

        rcv_finger();
        //show_finger();
    }

    // Waiting for everyone to be initialized before moving on
    MPI_Barrier(MPI_COMM_WORLD);

    if (p == 0) {
        // Randomly chooses a process initiateur to find the hash key
        int initiator = rand() % NB_PEERS;
        int key = rand() % (1 << M);
        int holder;

        MPI_Send(&key, 1, MPI_INT, initiator + 1, INITIATOR, MPI_COMM_WORLD);

        printf("The process (%d, %d) has to search for the key %d.\n", initiator + 1, nodes[initiator], key);

        // Waiting for the reception of the result
        MPI_Recv(&holder, 1, MPI_INT, MPI_ANY_SOURCE, FINISHED, MPI_COMM_WORLD, &status);

        printf("RESULT : %d is the holder of the key %d\n", holder, key);

        // Broadcasting a message FINISHED to stop all the processes
        for (int i = 1; i < nb_proc; i++) {
            MPI_Send(&holder, 1, MPI_INT, i, FINISHED, MPI_COMM_WORLD);
        }
    } else {
        receive();
    }

   MPI_Finalize();
   exit(EXIT_SUCCESS);

   failure:
   MPI_Finalize();
   exit(EXIT_FAILURE);
}
