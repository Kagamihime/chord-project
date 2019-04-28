#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <mpi.h>

#include "utils.h"

#define LOOSER 0
#define LEADER 1
#define UNKNOWN 2

#define TAGOUT 10
#define TAGIN 11
#define TAGCOLLECT 20
#define TAGANN 21

// This structure allows to easily pass the variables of the peer though
// function calls.
struct data {
    // Variables related to MPI
    int rk;
    int pred_rk;
    int succ_rk;
    int peers[NB_PEERS];

    // Variables related to the Hirschberg and Sinclair algorithm
    int init;
    int round;
    int nb_in;
    int state;

    // Variables related to Chord
    int id; // The Chord ID of the peer
    int finger_keys[M]; // The keys of each entries of the finger table.
    int finger_holders[M]; // The Chord IDs of the peers responsible for the
                           // keys of the matching entries of the finger table
};

// The function called for starting a round of the leader election algorithm
void init_round(struct data *d)
{
    int buff[3] = {0};

    d->nb_in = 0;

    if (d->round == 0) {
        d->init = 1;
    }

    buff[0] = d->rk; // The sender of this message: this process
    buff[1] = d->rk; // The initiator of the round: this process
    buff[2] = 1 << d->round; // The TTL of the message: 2^d->round

    MPI_Send(buff, 3, MPI_INT, d->pred_rk, TAGOUT, MPI_COMM_WORLD);
    MPI_Send(buff, 3, MPI_INT, d->succ_rk, TAGOUT, MPI_COMM_WORLD);

    d->round++;
}


// The function called for starting collecting the DHT's peers MPI ranks
// The election makes sure that this function is called by only one process:
// the leader
void init_peers_collect(struct data *d)
{
    int peers[NB_PEERS]; // The list of the peers' ranks
    int buff[NB_PEERS + 1];

    peers[0] = d->rk;

    for (int i = 1; i < NB_PEERS; i++) {
        peers[i] = -1; // An empty entry in the list has the value -1
    }

    buff[0] = d->rk; // The first entry of the buffer is the ID of the initiator
                     // of the collecting process

    // Next, the list of peers is appended to the buffer
    for (int i = 1; i < NB_PEERS + 1; i++) {
        buff[i] = peers[i - 1];
    }

    MPI_Send(buff, NB_PEERS + 1, MPI_INT, d->succ_rk, TAGCOLLECT, MPI_COMM_WORLD);
}

// The function for handling the reception of an OUT message of the election
// algorithm
void recv_out(struct data *d)
{
    MPI_Status status;
    int buff[3] = {0};
    int sender = 0, init = 0, dist = 0;

    MPI_Recv(buff, 3, MPI_INT, MPI_ANY_SOURCE, TAGOUT, MPI_COMM_WORLD, &status);
    sender = buff[0];
    init = buff[1];
    dist = buff[2];

    if (init > d->rk) {
        d->state = LOOSER;

        if (dist > 1) {
            buff[0] = d->rk;
            buff[1] = init;
            buff[2] = dist - 1;

            MPI_Send(buff, 3, MPI_INT, (sender == d->pred_rk) ? d->succ_rk : d->pred_rk, TAGOUT, MPI_COMM_WORLD);
        } else {
            buff[0] = d->rk;
            buff[1] = init;
            buff[2] = 0;

            MPI_Send(buff, 3, MPI_INT, (sender == d->pred_rk) ? d->pred_rk : d->succ_rk, TAGIN, MPI_COMM_WORLD);
        }
    } else if (init == d->rk && d->state != LEADER) {
        d->state = LEADER;
        init_peers_collect(d); // Initiate the collection of the peers' MPI
                               // ranks once the process is the leader
    } else if (!d->init) {
        // If the process has a higher rank but wasn't an initiator, it starts
        // participating to the election process
        d->init = 1;
    }
}

// The function for handling the reception of an IN message of the election
// algorithm
void recv_in(struct data *d)
{
    MPI_Status status;
    int buff[3] = {0};
    int sender = 0, init = 0, dist = 0;

    MPI_Recv(buff, 3, MPI_INT, MPI_ANY_SOURCE, TAGIN, MPI_COMM_WORLD, &status);
    sender = buff[0];
    init = buff[1];
    dist = buff[2];

    if (init != d->rk) {
        buff[0] = d->rk;
        buff[1] = init;
        buff[2] = dist;

        MPI_Send(buff, 3, MPI_INT, (sender == d->pred_rk) ? d->succ_rk : d->pred_rk, TAGOUT, MPI_COMM_WORLD);
    } else {
        if (++(d->nb_in) == 2) {
            init_round(d);
        }
    }
}

// The function for handling the reception of messages for collecting peers'
// MPI ranks
void recv_collect(struct data *d)
{
    MPI_Status status;
    int buff[NB_PEERS + 1] = {0};
    int init = 0, peers[NB_PEERS] = {0};

    MPI_Recv(buff, NB_PEERS + 1, MPI_INT, MPI_ANY_SOURCE, TAGCOLLECT, MPI_COMM_WORLD, &status);
    init = buff[0];

    for (int i = 0; i < NB_PEERS; i++) {
        peers[i] = buff[i + 1];
    }

    // Once the collection process is done, announce the list of peers
    if (init == d->rk) {
        for (int i = 0; i < NB_PEERS; i++) {
            d->peers[i] = peers[i];
        }

        buff[0] = d->rk;

        for (int i = 1; i < NB_PEERS + 1; i++) {
            buff[i] = d->peers[i - 1];
        }

        MPI_Send(buff, NB_PEERS + 1, MPI_INT, d->succ_rk, TAGANN, MPI_COMM_WORLD);
    } else {
        // Append the peer's rank to the list
        for (int i = 0; i < NB_PEERS; i++) {
            if (peers[i] == -1) {
                peers[i] = d->rk;
                break;
            }
        }

        buff[0] = init;

        for (int i = 1; i < NB_PEERS + 1; i++) {
            buff[i] = peers[i - 1];
        }

        MPI_Send(buff, NB_PEERS + 1, MPI_INT, d->succ_rk, TAGCOLLECT, MPI_COMM_WORLD);
    }
}

// The function for handling the reception of messages annoucing the list of
// all peers' MPI ranks
void recv_ann(struct data *d)
{
    MPI_Status status;
    int buff[NB_PEERS + 1] = {0};
    int init = 0;

    int hashed_peers[NB_PEERS] = {0}; // The list of all peers' Chord IDs

    MPI_Recv(buff, NB_PEERS + 1, MPI_INT, MPI_ANY_SOURCE, TAGANN, MPI_COMM_WORLD, &status);
    init = buff[0];

    // Pass on the annoucement to the successor and save the peers list, if we
    // are not the initiator
    if (init != d->rk) {
        for (int i = 0; i < NB_PEERS; i++) {
            d->peers[i] = buff[i + 1];
        }

        MPI_Send(buff, NB_PEERS + 1, MPI_INT, d->succ_rk, TAGANN, MPI_COMM_WORLD);
    }

    for (int i = 0; i < NB_PEERS; i++) {
        hashed_peers[i] = f(d->peers[i]);
    }

    qsort(hashed_peers, NB_PEERS, sizeof(int), cmpfunc);

    // Once we have the list of the peers' Chord IDs, we can build the finger table
    for (int i = 0; i < M; i++) {
        int entry = (d->id + (1 << i)) % (1 << M);
        int holder = -1;

        // Find the interval of two successive peers' Chord IDs the entry is
        // within. This takes into account the cyclic ordering.
        for (int j = 0; j < NB_PEERS; j++) {
            if ((entry > hashed_peers[j] && entry <= hashed_peers[(j + 1) % NB_PEERS])
               || (entry > hashed_peers[NB_PEERS - 1] || entry < hashed_peers[0])) {
                holder = hashed_peers[(j + 1) % NB_PEERS];
                break;
            }
        }

        d->finger_keys[i] = entry;
        d->finger_holders[i] = holder;
    }
}

// Main procedure called by the processes corresponding to the DHT's peers
void peer(int rk)
{
    // We must include the rank of the process in the seed of the RNG.
    // Otherwise, all processes will get the same numbers when deciding
    // whether or not they will be initiators of the calculation of the
    // finger table.
    srand(rk * time(NULL));

    MPI_Status status;
    struct data data = {
        .rk = rk,
        .pred_rk = (rk == 1) ? NB_PEERS : rk - 1,
        .succ_rk = (rk == NB_PEERS) ? 1 : rk + 1,
        .peers = {0},

        // The process 1 is always an initiator. This ensures there is always
        // at least one initiator.
        .init = (rk == 1) ? 1 : rand() % 2,
        .round = 0,
        .nb_in = 0,
        .state = UNKNOWN,

        .id = 0,
        .finger_keys = {0},
        .finger_holders = {0},
    };

    MPI_Recv(&data.id, 1, MPI_INT, 0, TAGINIT, MPI_COMM_WORLD, &status);
    printf("%d Chord ID is %d\n", data.rk, data.id);

    for (;;) {
        // Start the first round of the election algorithm if the process is an
        // initiator or when it starts participating to it. This can be called
        // at most once per process.
        if (data.init && data.round == 0) {
            init_round(&data);
        }

        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        if (status.MPI_TAG == TAGOUT) {
            recv_out(&data);
        } else if (status.MPI_TAG == TAGIN) {
            recv_in(&data);
        } else if (status.MPI_TAG == TAGCOLLECT) {
            recv_collect(&data);
        } else if (status.MPI_TAG == TAGANN) {
            recv_ann(&data);
            break; // Leave the loop when the finger table is built
        } else {
            int ignore;

            MPI_Recv(&ignore, 1, MPI_INT, MPI_ANY_SOURCE, TAGINIT, MPI_COMM_WORLD, &status);
        }
    }

    for (int i = 0; i < M; i++) {
        printf("%d: Entry %d: Key: %d / Holder: %d\n", data.rk, i, data.finger_keys[i], data.finger_holders[i]);
    }
}

int main(int argc, char *argv[])
{
    int nb_proc, rk;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nb_proc);

    if (nb_proc != NB_PEERS + 1) {
        fprintf(stderr, "Incorrect number of processes!\n");
        goto failure;
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rk);

    if (rk == 0) {
        simulator();
    } else {
        peer(rk);
    }

    MPI_Finalize();
    exit(EXIT_SUCCESS);

    failure:
    MPI_Finalize();
    exit(EXIT_FAILURE);
}
