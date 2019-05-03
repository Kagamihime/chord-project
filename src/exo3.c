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
#define LOOKUP 4
#define FINISHED 5

// Tags added for the joining process
#define NOTIFY 6
#define JOINED 7

/* the maximum of keys one site can have */
#define MAX_CAPACITY 10

/* NODE SPECIFIC VARIABLES */
int p; /* rank of the site */
int id_p; /*chord identifier f(p) */
ID finger_p[M]; /* finger table of p */
int reverse_p[NB_PEERS]; // MPI ranks of the peers having this site in their finger tables
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
    int next_peers[NB_PEERS][M];
    int rev_lists[NB_PEERS][NB_PEERS];

    for (int i = 0; i < NB_PEERS; i++) {
        for (int j = 0; j < M; j++) {
            next_peers[i][j] = -1;
        }
    }

    for (int i = 0; i < NB_PEERS; i++) {
        for (int j = 0; j < NB_PEERS; j++) {
            rev_lists[i][j] = -1;
        }
    }

    for (int i = 0; i < NB_PEERS; i++) {
        nodes[i] = rand() % max;
    }

    qsort(nodes, NB_PEERS, sizeof(int), cmpfunc);

    for (int i = 0; i < NB_PEERS; i++) {
        printf("Node MPI rank: %d. Node Chord ID: %d\n", i + 1, nodes[i]);
    }

    for (int i = 0; i < NB_PEERS; i++) {
        int id = nodes[i];

        MPI_Send(&id, 1, MPI_INT, i + 1, INIT, MPI_COMM_WORLD);

        for (int j = 0; j < M; j++) {
            int val = (nodes[i] + (1 << j)) % (1 << M);
            int respo = find_responsible(val) + 1;

            MPI_Send(&respo, 1, MPI_INT, i + 1, INIT, MPI_COMM_WORLD);
            MPI_Send(&nodes[respo - 1], 1, MPI_INT, i + 1, INIT, MPI_COMM_WORLD);

            next_peers[i][j] = respo;
        }
    }

    // This is for sending the reverse_p tables for each peers
    for (int rev_list_idx = 0; rev_list_idx < NB_PEERS; rev_list_idx++) {
        for (int next_peers_idx = 0; next_peers_idx < NB_PEERS; next_peers_idx++) {
            for (int i = 0; i < M; i++) {
                if (next_peers[next_peers_idx][i] - 1 == rev_list_idx) {
                    for (int j = 0; j < NB_PEERS; j++) {
                        if (rev_lists[rev_list_idx][j] == -1) {
                            rev_lists[rev_list_idx][j] = next_peers_idx + 1;
                            break;
                        }
                    }

                    break;
                }
            }
        }

        MPI_Send(&rev_lists[rev_list_idx], NB_PEERS, MPI_INT, rev_list_idx + 1, INIT, MPI_COMM_WORLD);
    }
}

// The function used by the nodes to received the values
// of their finger tables that the master process
// has sent them
void recv_finger()
{
    MPI_Status status;

    for (int i = 0; i < M; i++) {
        int val = 0, hash = 0;

        MPI_Recv(&val, 1, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);
        finger_p[i].id = val;

        MPI_Recv(&hash, 1, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);
        finger_p[i].hash = hash;
    }

    succ_p = finger_p[0];
}

// This function is used by the peers to receive their reverse_p tables from
// the master process
void recv_reverse()
{
    MPI_Status status;

    MPI_Recv(&reverse_p, NB_PEERS, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);
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

    for (int i = M - 1; i >= 0; i--) {
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
    for (int i = 0; i < M; i++) {
        if (finger_p[i].id == rank) {
            return 1;
        }
    }

    return 0;
}

// Main procedure of the DHT's peers
void receive()
{
    MPI_Status status;

    for (;;) {
        int hash, caller, result, holder, holder_rk;
        int lookup_caller; // Used for routing back to the peer which called `lookup`
        ID dest;

        int sender;
        ID new_peer;

        MPI_Recv(&hash, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        switch (status.MPI_TAG) {
            case FORWARD:
                // Message received if a process forwarded to p the search of the key

                MPI_Recv(&caller, 1, MPI_INT, status.MPI_SOURCE, FORWARD, MPI_COMM_WORLD, &status);

                // it must find the next process to send it to

                ID target = find_next(hash);

                if (status.MPI_SOURCE == p) {
                    printf("problem\n");

                    MPI_Send(&id_p, 1, MPI_INT, 0, FINISHED, MPI_COMM_WORLD);
                }

                if (target.hash == -1) {
                    // If the key isn't included in any of the finger table's intervals
                    // the successor is in charge of the key, we must warn it

                    MPI_Send(&hash, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                } else {
                    // else juste forward the request to the process found earlier

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

                    MPI_Send(&hash, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&id_p, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&p, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&result, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                } else {
                    // If we initiated the search, we can warn the master the process that the algorithm is over

                    MPI_Send(&id_p, 1, MPI_INT, lookup_caller, FINISHED, MPI_COMM_WORLD);
                    MPI_Send(&p, 1, MPI_INT, lookup_caller, FINISHED, MPI_COMM_WORLD);
                }

                break;

            case RESULT:
                MPI_Recv(&caller, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);
                MPI_Recv(&holder, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);
                MPI_Recv(&holder_rk, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);
                MPI_Recv(&result, 1, MPI_INT, status.MPI_SOURCE, RESULT, MPI_COMM_WORLD, &status);

                // Message received when the result of the search is trying
                // to come back to the caller

                if (caller == id_p) {
                    // if we happen to be the caller, the search is over

                    MPI_Send(&holder, 1, MPI_INT, lookup_caller, FINISHED, MPI_COMM_WORLD);
                    MPI_Send(&holder_rk, 1, MPI_INT, lookup_caller, FINISHED, MPI_COMM_WORLD);
                } else {
                    // Else we must forward the result in the usual manner

                    ID target = find_next(caller);

                    if (target.hash == id_p) {
                        MPI_Send(&holder, 1, MPI_INT, lookup_caller, FINISHED, MPI_COMM_WORLD);
                        MPI_Send(&holder_rk, 1, MPI_INT, lookup_caller, FINISHED, MPI_COMM_WORLD);
                    }

                    MPI_Send(&hash, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&caller, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&holder, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&holder_rk, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                    MPI_Send(&result, 1, MPI_INT, target.id, RESULT, MPI_COMM_WORLD);
                }

                break;

            case LOOKUP:
                // Message received when a peer needs to search for the holder
                // of a key

                lookup_caller = status.MPI_SOURCE;

                // Searches to whom the next message must be sent
                dest = find_next(hash);

                if (dest.hash  == -1) {
                    // the successor is in charge of the key we are looking for

                    MPI_Send(&hash, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                    MPI_Send(&id_p, 1, MPI_INT, succ_p.id, SEARCH, MPI_COMM_WORLD);
                } else {
                    // just forwarding the request to the next target

                    MPI_Send(&hash, 1, MPI_INT, dest.id, FORWARD, MPI_COMM_WORLD);
                    MPI_Send(&id_p, 1, MPI_INT, dest.id, FORWARD, MPI_COMM_WORLD);
                }

                break;

            case NOTIFY:
                // Message used in the joining process to indicate that the
                // entry point must notify the peers in reverse_p of the
                // existence of a new peer

                for (int i = 0; i < NB_PEERS; i++) {
                    if (reverse_p[i] == -1) {
                        break;
                    }

                    MPI_Send(&id_p, 1, MPI_INT, reverse_p[i], JOINED, MPI_COMM_WORLD);
                    MPI_Send(&status.MPI_SOURCE, 1, MPI_INT, reverse_p[i], JOINED, MPI_COMM_WORLD);
                    MPI_Send(&hash, 1, MPI_INT, reverse_p[i], JOINED, MPI_COMM_WORLD);
                }

                MPI_Send(&id_p, 1, MPI_INT, 0, FINISHED, MPI_COMM_WORLD);

                break;

            case JOINED:
                // Message used to indicate a peer that they may need to update
                // their finger table, because a new peer joined the DHT which
                // may be the new holder of one of their finger table's entries

                sender = hash;

                MPI_Recv(&new_peer.id, 1, MPI_INT, MPI_ANY_SOURCE, JOINED, MPI_COMM_WORLD, &status);
                MPI_Recv(&new_peer.hash, 1, MPI_INT, MPI_ANY_SOURCE, JOINED, MPI_COMM_WORLD, &status);

                printf("(%d,%d) has been informed of the existence of (%d,%d). Printing finger table:\n", p, id_p, new_peer.id, new_peer.hash);

                for (int i = 0; i < M; i++) {
                    int key = (id_p + (1 << i)) % (1 << M);
                    ID holder = finger_p[i];

                    if (holder.hash == sender) {
                        if (new_peer.hash < holder.hash) {
                            if (!(key > new_peer.hash && key <= holder.hash)) {
                                finger_p[i] = new_peer;
                            }
                        } else {
                            if (!(key <= holder.hash || key > new_peer.hash)) {
                                finger_p[i] = new_peer;
                            }
                        }
                    }

                    printf("(%d,%d) entry %d holder (%d,%d)\n", p, id_p, key, finger_p[i].id, finger_p[i].hash);
                }

                break;

            case FINISHED:
                // Message received from the master process whenever the process
                // should stop

                return;
            default:
                printf("process %d has received an unknown message \n", p);
        }
    }
}

// Main procedure of the new peer which joins the DHT
void join_dht(int rk, int entry_rk)
{
    MPI_Status status;

    int id = f(rk);
    int succ = -1;
    int succ_rk = -1;
    int finger_keys[M];
    ID finger_holders[M];

    for (int i = 0; i < M; i++) {
        finger_keys[i] = -1;
    }

    MPI_Send(&id, 1, MPI_INT, entry_rk, LOOKUP, MPI_COMM_WORLD);
    MPI_Recv(&succ, 1, MPI_INT, entry_rk, FINISHED, MPI_COMM_WORLD, &status);
    MPI_Recv(&succ_rk, 1, MPI_INT, entry_rk, FINISHED, MPI_COMM_WORLD, &status);

    for (int i = 0; i < M; i++) {
        int entry = (id + (1 << i)) % (1 << M);
        int holder = -1, holder_rk = -1;

        MPI_Send(&entry, 1, MPI_INT, entry_rk, LOOKUP, MPI_COMM_WORLD);
        MPI_Recv(&holder, 1, MPI_INT, entry_rk, FINISHED, MPI_COMM_WORLD, &status);
        MPI_Recv(&holder_rk, 1, MPI_INT, entry_rk, FINISHED, MPI_COMM_WORLD, &status);

        for (int i = 0; i < M; i++) {
            if (finger_keys[i] == -1) {
                finger_keys[i] = entry;
                finger_holders[i].id = holder_rk;
                finger_holders[i].hash = holder;

                break;
            }
        }
    }

    printf("\nFinger table of the new peer (%d,%d):\n", rk, id);

    for (int i = 0; i < M; i++) {
        printf("(%d,%d) entry %d holder (%d,%d)\n", rk, id, finger_keys[i], finger_holders[i].id, finger_holders[i].hash);
    }

    printf("\n");

    MPI_Send(&id, 1, MPI_INT, succ_rk, NOTIFY, MPI_COMM_WORLD);
}

int main(int argc, char *argv[])
{
    int nb_proc;
    MPI_Status status;

    srand(time(NULL));

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nb_proc);

    if (nb_proc != NB_PEERS + 2) {
        fprintf(stderr, "Incorrect number of processes!\n");
        fprintf(stderr, "There should be %d\n", NB_PEERS + 2);
        goto failure;
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &p);

    if (p == 0) {
        simulateur();
    } else if (p != nb_proc - 1) {
        MPI_Recv(&id_p, 1, MPI_INT, 0, INIT, MPI_COMM_WORLD, &status);

        recv_finger();
        recv_reverse();
    }

    // Waiting for everyone to be initialized before moving on
    MPI_Barrier(MPI_COMM_WORLD);

    if (p == 0) {
        int new_peer_id;

        // Waiting for the reception of the result
        MPI_Recv(&new_peer_id, 1, MPI_INT, MPI_ANY_SOURCE, FINISHED, MPI_COMM_WORLD, &status);

        // Broadcasting a message FINISHED to stop all the processes
        for (int i = 1; i < nb_proc; i++) {
            MPI_Send(&new_peer_id, 1, MPI_INT, i, FINISHED, MPI_COMM_WORLD);
        }
    } else if (p == nb_proc - 1) {
        join_dht(p, 1);
    } else {
        receive();
    }

   MPI_Finalize();
   exit(EXIT_SUCCESS);

   failure:
   MPI_Finalize();
   exit(EXIT_FAILURE);
}
