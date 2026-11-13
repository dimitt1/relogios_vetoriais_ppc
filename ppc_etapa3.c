/*
 * Etapa 3 - Relogios Vetoriais + Produtor/Consumidor
 * Disciplina: Programacao Paralela e Concorrente
 * Baseado nos fluxos e requisitos do "Diagrama da Etapa 3".
 *
 * Compilar: mpicc -std=c11 -O2 -Wall -Wextra -Wpedantic -pthread -o etapa3 etapa3.c
 * Executar: mpiexec -n 3 ./etapa3
 *
 * Cada processo MPI tem exatamente 3 threads durante a simulacao:
 *   - thread principal/central: EVENT, SEND (enfileirar), RECEIVE (desenfileirar)
 *   - thread emissora: desenfileirar e executar MPI_Send
 *   - thread receptora: executar MPI_Recv e enfileirar
 *
 * As filas de envio e de recepcao sao independentes, cada uma com seu
 * proprio mutex e duas variaveis de condicao (not_full, not_empty).
 */

#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NPROCS 3
#define QUEUE_CAPACITY 8
#define CLOCK_TAG 17

typedef struct {
    int v[NPROCS];
} Clock;

typedef struct {
    Clock clock;
    int peer;       /* destino na fila de envio; origem na fila de recepcao */
} Message;

typedef struct {
    Message data[QUEUE_CAPACITY];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} Queue;

typedef struct {
    int rank;
    int send_total;
    int receive_total;
    Clock local_clock;           /* acessado somente pela thread central */
    Queue outgoing;
    Queue incoming;
    pthread_mutex_t print_mutex;
} Process;

static void check_pthread(int error, const char *operation) {
    if (error != 0) {
        fprintf(stderr, "Falha em %s: %s\n", operation, strerror(error));
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }
}

static void queue_init(Queue *q) {
    memset(q, 0, sizeof(*q));
    check_pthread(pthread_mutex_init(&q->mutex, NULL), "pthread_mutex_init");
    check_pthread(pthread_cond_init(&q->not_full, NULL), "pthread_cond_init(not_full)");
    check_pthread(pthread_cond_init(&q->not_empty, NULL), "pthread_cond_init(not_empty)");
}

static void queue_destroy(Queue *q) {
    check_pthread(pthread_cond_destroy(&q->not_full), "pthread_cond_destroy(not_full)");
    check_pthread(pthread_cond_destroy(&q->not_empty), "pthread_cond_destroy(not_empty)");
    check_pthread(pthread_mutex_destroy(&q->mutex), "pthread_mutex_destroy");
}

/* Produtor da fila: aguarda uma vaga sem reter o mutex durante a espera. */
static void queue_push(Queue *q, Message msg) {
    check_pthread(pthread_mutex_lock(&q->mutex), "pthread_mutex_lock");
    while (q->count == QUEUE_CAPACITY) {
        check_pthread(pthread_cond_wait(&q->not_full, &q->mutex), "pthread_cond_wait(not_full)");
    }
    q->data[q->tail] = msg;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY;
    q->count++;
    check_pthread(pthread_cond_signal(&q->not_empty), "pthread_cond_signal(not_empty)");
    check_pthread(pthread_mutex_unlock(&q->mutex), "pthread_mutex_unlock");
}

/* Consumidor da fila: aguarda um item sem ocupar a CPU em busy-wait. */
static Message queue_pop(Queue *q) {
    check_pthread(pthread_mutex_lock(&q->mutex), "pthread_mutex_lock");
    while (q->count == 0) {
        check_pthread(pthread_cond_wait(&q->not_empty, &q->mutex), "pthread_cond_wait(not_empty)");
    }
    Message msg = q->data[q->head];
    q->head = (q->head + 1) % QUEUE_CAPACITY;
    q->count--;
    check_pthread(pthread_cond_signal(&q->not_full), "pthread_cond_signal(not_full)");
    check_pthread(pthread_mutex_unlock(&q->mutex), "pthread_mutex_unlock");
    return msg;
}

static void print_clock(Process *p, const char *action) {
    check_pthread(pthread_mutex_lock(&p->print_mutex), "pthread_mutex_lock(print)");
    printf("[P%d][CENTRAL] %-32s -> (%d,%d,%d)\n",
           p->rank, action,
           p->local_clock.v[0], p->local_clock.v[1], p->local_clock.v[2]);
    fflush(stdout);
    check_pthread(pthread_mutex_unlock(&p->print_mutex), "pthread_mutex_unlock(print)");
}

/* A thread central e a unica que modifica local_clock. */
static void event(Process *p) {
    p->local_clock.v[p->rank]++;
    print_clock(p, "EVENT");
}

/* SEND logico: registra o evento e adiciona snapshot a fila de envio. */
static void send_clock(Process *p, int destination) {
    p->local_clock.v[p->rank]++;
    Message msg = {.clock = p->local_clock, .peer = destination};
    queue_push(&p->outgoing, msg);
    char action[64];
    snprintf(action, sizeof(action), "SEND(%d) -> fila de envio", destination);
    print_clock(p, action);
}

/* RECEIVE logico: retira qualquer mensagem da fila, funde max e incrementa. */
static void receive_clock(Process *p) {
    Message msg = queue_pop(&p->incoming);
    for (int i = 0; i < NPROCS; i++) {
        if (msg.clock.v[i] > p->local_clock.v[i]) {
            p->local_clock.v[i] = msg.clock.v[i];
        }
    }
    p->local_clock.v[p->rank]++;
    char action[64];
    snprintf(action, sizeof(action), "RECEIVE da fila (origem P%d)", msg.peer);
    print_clock(p, action);
}

static void *sender_thread(void *arg) {
    Process *p = (Process *)arg;
    for (int i = 0; i < p->send_total; i++) {
        Message msg = queue_pop(&p->outgoing);
        int status = MPI_Send(msg.clock.v, NPROCS, MPI_INT,
                              msg.peer, CLOCK_TAG, MPI_COMM_WORLD);
        if (status != MPI_SUCCESS) {
            fprintf(stderr, "[P%d] Falha em MPI_Send\n", p->rank);
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }
        check_pthread(pthread_mutex_lock(&p->print_mutex), "lock(print)");
        printf("[P%d][EMISSORA] MPI_Send -> P%d: (%d,%d,%d)\n",
               p->rank, msg.peer,
               msg.clock.v[0], msg.clock.v[1], msg.clock.v[2]);
        fflush(stdout);
        check_pthread(pthread_mutex_unlock(&p->print_mutex), "unlock(print)");
    }
    return NULL;
}

static void *receiver_thread(void *arg) {
    Process *p = (Process *)arg;
    for (int i = 0; i < p->receive_total; i++) {
        Message msg;
        MPI_Status mpi_status;
        int status = MPI_Recv(msg.clock.v, NPROCS, MPI_INT,
                              MPI_ANY_SOURCE, CLOCK_TAG,
                              MPI_COMM_WORLD, &mpi_status);
        if (status != MPI_SUCCESS) {
            fprintf(stderr, "[P%d] Falha em MPI_Recv\n", p->rank);
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }
        msg.peer = mpi_status.MPI_SOURCE;
        queue_push(&p->incoming, msg);
        check_pthread(pthread_mutex_lock(&p->print_mutex), "lock(print)");
        printf("[P%d][RECEPTORA] MPI_Recv de P%d -> fila: (%d,%d,%d)\n",
               p->rank, msg.peer,
               msg.clock.v[0], msg.clock.v[1], msg.clock.v[2]);
        fflush(stdout);
        check_pthread(pthread_mutex_unlock(&p->print_mutex), "unlock(print)");
    }
    return NULL;
}

/* Ordem exata das operacoes especificadas pelo diagrama da Etapa 3. */
static void central_thread(Process *p) {
    print_clock(p, "Estado inicial");
    switch (p->rank) {
        case 0:
            event(p);
            send_clock(p, 1);
            receive_clock(p);
            send_clock(p, 2);
            receive_clock(p);
            send_clock(p, 1);
            event(p);
            break;
        case 1:
            send_clock(p, 0);
            receive_clock(p);
            receive_clock(p);
            break;
        case 2:
            event(p);
            send_clock(p, 0);
            receive_clock(p);
            break;
    }
}

int main(int argc, char **argv) {
    int provided = 0;
    int init_status = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (init_status != MPI_SUCCESS) {
        fprintf(stderr, "Falha ao inicializar MPI.\n");
        return EXIT_FAILURE;
    }
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr,
                "MPI_THREAD_MULTIPLE nao disponivel (nivel oferecido: %d).\n"
                "Esta atividade exige MPI_Send e MPI_Recv em threads distintas.\n", provided);
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (size != NPROCS) {
        if (rank == 0) {
            fprintf(stderr, "Execute com exatamente 3 processos: mpiexec -n 3 ./etapa3\n");
        }
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    /* Total conhecido pelo diagrama: P0 envia 3/recebe 2;
       P1 envia 1/recebe 2; P2 envia 1/recebe 1. Isso permite
       encerrar os workers sem sentinelas nem chamadas MPI extras. */
    static const int send_counts[NPROCS] = {3, 1, 1};
    static const int receive_counts[NPROCS] = {2, 2, 1};
    Process p = {0};
    p.rank = rank;
    p.send_total = send_counts[rank];
    p.receive_total = receive_counts[rank];
    queue_init(&p.outgoing);
    queue_init(&p.incoming);
    check_pthread(pthread_mutex_init(&p.print_mutex, NULL), "pthread_mutex_init(print)");

    pthread_t sender, receiver;
    check_pthread(pthread_create(&receiver, NULL, receiver_thread, &p),
                  "pthread_create(receiver)");
    check_pthread(pthread_create(&sender, NULL, sender_thread, &p),
                  "pthread_create(sender)");

    /* A thread principal do processo MPI e a terceira thread: a CENTRAL. */
    central_thread(&p);

    check_pthread(pthread_join(sender, NULL), "pthread_join(sender)");
    check_pthread(pthread_join(receiver, NULL), "pthread_join(receiver)");
    print_clock(&p, "Estado final do processo");

    queue_destroy(&p.incoming);
    queue_destroy(&p.outgoing);
    check_pthread(pthread_mutex_destroy(&p.print_mutex), "pthread_mutex_destroy(print)");

    MPI_Finalize();
    return EXIT_SUCCESS;
}
