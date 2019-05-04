# chord-project
A repository for the project of the AR ("Algorithmique Répartie") course at Sorbonne University.

## Usage

### Exercice 1

In order to use the implementation of the first exercice,
you'll need to be in the project's root directory and run:

```
make exo1
```

Then to run the program, execute:

```
mpirun -np 7 ./exo1
```

### Exercice 2:

To use the implementation of the second exercice,
you'll need to be in the project's root directory and run:

```
make init_dht
```

Then you can run it with:

```
mpirun -np 7 ./init_dht
```


### Exercice 3:

To use the implementation of the third exercice, you'll
need to be in the project's root directory and run:

```
make exo3
```

Then you can run it with (you may need to run it several times):

```
mpirun -np 8 ./exo3
```
