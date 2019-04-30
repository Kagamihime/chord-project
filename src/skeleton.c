#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "utils.h"

void peer(int rk)
{
    MPI_Status status;

    int succ_rk = (rk == NB_PEERS) ? 1 : rk + 1;
    int id = 0, succ_id = 0;

    MPI_Recv(&id, 1, MPI_INT, 0, TAGINIT, MPI_COMM_WORLD, &status);
    MPI_Recv(&succ_id, 1, MPI_INT, 0, TAGINIT, MPI_COMM_WORLD, &status);

    printf("(%d, %d): successor (%d, %d)\n", rk, id, succ_rk, succ_id);
}

int main(int argc, char *argv[])
{
    int nb_proc, rk;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nb_proc);

    if (nb_proc != NB_PEERS + 1) {
        fprintf(stderr, "Incorrect number of processes!\n");
        fprintf(stderr, "There should be %d\n", NB_PEERS + 1);
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
