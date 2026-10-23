/* File:  
 *    prod_con.c
 *
 * Purpose:
 *    Implementação de um pool de relogios vetoriais (2a etapa do projeto)
 *
 *
 * Compile:  gcc -g -Wall -o prod_con prod_con.c -lpthread -lrt
 * Usage:    ./prod_con x (x sendo o modo do programa: 1 - Fila cheia, 2 - Fila vazia, 3 - Normal)
 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h> 
#include <unistd.h>
#include <semaphore.h>
#include <time.h>

#define THREAD_NUM 3    // Tamanho do pool de threads
#define BUFFER_SIZE 20 // Númermo máximo de tarefas enfileiradas
#define max(x,y) (x > y ? x : y)

/* Estrutura do relógio vetorial (3 processos). */
typedef struct Clock {
    int p[3];
} Clock;

Clock clockQueue[BUFFER_SIZE];
Clock clocks[THREAD_NUM];
int clockCount = 0;

pthread_mutex_t mutex;

pthread_cond_t condFull;
pthread_cond_t condEmpty;

// Um mutex por relogio (mesma quantidade de threads)
pthread_mutex_t clockMutex[THREAD_NUM];

// send eh o evento dos produtores
Clock sendEvent(int id){
   pthread_mutex_lock(&clockMutex[id]);
   
   clocks[id].p[id]++;
   Clock msg = clocks[id];
   printf("(Produtor %d) Enviou para a fila: (%d,%d,%d)\n", id, msg.p[0], msg.p[1], msg.p[2]);

   pthread_mutex_unlock(&clockMutex[id]);
   return msg;
}

// receive eh o evento dos consumidores
void receiveClock(Clock msg, int id){
   pthread_mutex_lock(&clockMutex[id]);
   
   for (int k = 0; k < 3; k++){
      clocks[id].p[k] = max(clocks[id].p[k], msg.p[k]);
   }
   clocks[id].p[id]++;
   printf("(Consumidor %d) Consumiu (%d,%d,%d) da fila -> Relogio atual: (%d,%d,%d)\n",
          id, msg.p[0], msg.p[1], msg.p[2],
          clocks[id].p[0], clocks[id].p[1], clocks[id].p[2]);
   
   pthread_mutex_unlock(&clockMutex[id]);
}

Clock getClock(){
   pthread_mutex_lock(&mutex);
   
   while (clockCount == 0){
      printf("Fila vazia! Aguardando...\n");
      pthread_cond_wait(&condEmpty, &mutex);
   }
   
   Clock clock = clockQueue[0];
   int i;
   for (i = 0; i < clockCount - 1; i++){
      clockQueue[i] = clockQueue[i+1];
   }
   clockCount--;
   
   pthread_mutex_unlock(&mutex);
   pthread_cond_signal(&condFull);
   return clock;
}

void submitClock(int id){
   pthread_mutex_lock(&mutex);

   while (clockCount == BUFFER_SIZE){
      printf("Fila cheia! Aguardando...\n");
      pthread_cond_wait(&condFull, &mutex);
   }

   Clock clock = sendEvent(id);
   clockQueue[clockCount] = clock;
   clockCount++;

   pthread_mutex_unlock(&mutex);
   pthread_cond_signal(&condEmpty);
}

void *startProd(void* args);
void *startCon(void* args);

/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
   if(argc < 2){
      printf("Erro! Especifique o modo de teste:\n1 - Fila cheia\n2 - Fila vazia\n3 - Normal\n");
      return 0;
   }

   int op = atoi(argv[1]);
   if(op < 1 || op > 3){
      printf("Opcao invalida! Tente novamente!\n");
      return 0;
   }

   // inicializando 3 relogios vetoriais zerados.
   for(int i = 0; i < THREAD_NUM; i++){
      for(int j = 0; j < 3; j++){
         clocks[i].p[j] = 0;
      }
   }

   pthread_mutex_init(&mutex, NULL);
   pthread_cond_init(&condEmpty, NULL);
   pthread_cond_init(&condFull, NULL);
   pthread_t threadProd[THREAD_NUM],threadCon[THREAD_NUM]; 
   long i;
   for (i = 0; i < THREAD_NUM; i++) pthread_mutex_init(&clockMutex[i], NULL);

   // tempo (s) entre iteracoes de produtores e consumidores em cada modo
   int tProd, tCon;
   if(op == 1){ tProd = 1; tCon = 2; }       // fila fica cheia
   else if(op == 2){ tProd = 2; tCon = 1; }  // fila fica vazia
   else { tProd = 1; tCon = 1; }             // caso normal

   printf("Iniciando simulacao do tipo %d\n",op);
   // array argumentos pra enviar (id e tempo) 
   int prodArgs[THREAD_NUM][2], conArgs[THREAD_NUM][2];

   // inicializando os 2 pacotes de 3 threads
   for (i = 0; i < THREAD_NUM; i++){
      prodArgs[i][0] = i; prodArgs[i][1] = tProd;
      if (pthread_create(&threadProd[i], NULL, &startProd, (void*) prodArgs[i]) != 0) {
         perror("Failed to create the thread");
      }
   }
   for (i = 0; i < THREAD_NUM; i++){
      conArgs[i][0] = i; conArgs[i][1] = tCon;
      if (pthread_create(&threadCon[i], NULL, &startCon, (void*) conArgs[i]) != 0) {
         perror("Failed to create the thread");
      }
   }
   

   for (i = 0; i < THREAD_NUM; i++){  
      if (pthread_join(threadProd[i], NULL) != 0) {
         perror("Failed to join the thread");
      }
   }
   
   pthread_mutex_destroy(&mutex);
   pthread_cond_destroy(&condEmpty);
   pthread_cond_destroy(&condFull);
   return 0;
}  /* main */

/*-------------------------------------------------------------------*/
void *startCon(void* args) {
   int* v = (int *) args; 
   int id = v[0], t = v[1];
   while (1){
      Clock msg = getClock();
      receiveClock(msg, id);
      sleep(t);
   }
   return NULL;
}

void *startProd(void* args) {
   int* v = (int *) args;
   int id = v[0], t = v[1];
   while (1){
      submitClock(id);
      sleep(t);
   }
   return NULL;
}