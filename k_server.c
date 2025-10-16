#include <stdio.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>

#define MAX_TOKENS 3
#define MAX_THREADS 10

//declaring variables

int sockfd, newsockfd, portno; 
socklen_t clilen;
struct sockaddr_in serv_addr, cli_addr;
int n1, n2;
char *tokens[MAX_TOKENS];
char buffer[256];
int count,found;
pthread_t threads[MAX_THREADS];
int threads_counter = 0;
int active_connection = 0;

// linked list for storing data

struct node{
    char *data;
    int key;
    struct node *next;
};

struct node *head = NULL;

// queue for worker thread pool

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int front = 0, rear = 0, elements_count = 0;
int queue[MAX_THREADS];
void enqueue(int* queue, int socket);
int dequeue(int* queue);


// declaring functions

void error(char *msg);
bool search(struct node *head, int val);
struct node *find_node(int val, int *found);
void create(int key, int value_size, int newsockfd);
void update(int key, int value_size, int newsockfd);
void reads(int key, int newsockfd);
void delete(int key, int newsockfd);
void *thread_func(void *newsockfd);

// main function

int main(int argc, char* argv[]){
    
    if (argc < 2) {
        fprintf(stderr,"ERROR, no port provided\n");
        exit(1);
    }

    // creating a socket 

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if(sockfd < 0)error("ERROR opening socket"); 

    // filling in server address details

    bzero((char *) &serv_addr, sizeof(serv_addr));
    portno = atoi(argv[1]);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);

    // binding the socket

    if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
        error("ERROR on binding");

    // listen for new connections

    if(listen(sockfd,MAX_THREADS) < 0)error("ERROR listening");
    clilen = sizeof(cli_addr); 

    // spawn new threads

    for(int inc = 0 ; inc < MAX_THREADS ; inc++){
        pthread_create(&threads[inc], NULL, thread_func, NULL);
    }

    // handling new connections

    while(1){

        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) 
             error("ERROR on accept");

        printf("New client connection has been requested\n");
        enqueue(queue, newsockfd);

        /*
        newsockfd[threads_counter] = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd[threads_counter] < 0) 
             error("ERROR on accept");

        pthread_create(&threads[threads_counter],NULL, thread_func, &newsockfd[threads_counter]);
        pthread_detach(threads[threads_counter]);
        active_connection++;
        threads_counter = (threads_counter + 1)%MAX_THREADS;*/
    }
}

// other functions

void *thread_func(void *param){

    while(1){
        int newsockfd = dequeue(queue);

            printf("New connection has been accepted from a client\n");

            while(1){
           
            // read meta data packet from the server
            n1 = read(newsockfd,buffer,255);
                if(n1 == 0){
                    printf("The client has disconnected\n");
                    close(newsockfd);
                    break;
                }
                else if(n1 < 0){
                    close(newsockfd);
                    error("ERROR reading from socket");
                } 
          
            // tokenize the meta-data
            count = 0;
            char *token = strtok(buffer," ");
            while(token != NULL && count < MAX_TOKENS){
                tokens[count++] = token;
                // printf("%s\n",token);
                token = strtok(NULL," ");
            }
            // CRUD operation handler
            if(strcmp("create",tokens[0]) == 0){
                printf("create function for key %s executed\n",tokens[1]);
                create(atoi(tokens[1]),atoi(tokens[2]), newsockfd);
            }
            else if(strcmp("update",tokens[0]) == 0){
                printf("update function for key %s executed\n",tokens[1]);
                update(atoi(tokens[1]),atoi(tokens[2]), newsockfd);
            }
            else if(strcmp("read",tokens[0]) == 0){
                printf("read function for key %s executed\n",tokens[1]);
                reads(atoi(tokens[1]), newsockfd);
            }
            else if(strcmp("delete",tokens[0]) == 0){
                printf("delete function for key %s executed\n",tokens[1]);
                delete(atoi(tokens[1]), newsockfd);
            }
            else if(strcmp("disconnect",tokens[0]) == 0){
                if(close(newsockfd) == 0){
                    printf("\nConnection terminated\n");
                    break;
                }
            }
        }
    }   
    return 0;


    // printf("Connection accepted for a new client\n");

    // while(1 /*connection established per handle */ ){
       
    //     int newsockfd = *(int *)socket;

    //     // read meta data packet from the server
    //     n1 = read(newsockfd,buffer,255);
    //         if(n1 == 0){
    //             printf("The client has disconnected\n");
    //             close(newsockfd);
    //             break;
    //         }
    //         else if(n1 < 0){
    //             close(newsockfd);
    //             error("ERROR reading from socket");
    //         } 
        
    //     // tokenize the meta-data
    //     count = 0;
    //     char *token = strtok(buffer," ");
    //     while(token != NULL && count < MAX_TOKENS){
    //         tokens[count++] = token;
    //         // printf("%s\n",token);
    //         token = strtok(NULL," ");
    //     }
    //     // CRUD operation handler
    //     if(strcmp("create",tokens[0]) == 0){
    //         printf("create function for key %s executed\n",tokens[1]);
    //         create(atoi(tokens[1]),atoi(tokens[2]), newsockfd);
    //     }
    //     else if(strcmp("update",tokens[0]) == 0){
    //         printf("update function for key %s executed\n",tokens[1]);
    //         update(atoi(tokens[1]),atoi(tokens[2]), newsockfd);
    //     }
    //     else if(strcmp("read",tokens[0]) == 0){
    //         printf("read function for key %s executed\n",tokens[1]);
    //         reads(atoi(tokens[1]), newsockfd);
    //     }
    //     else if(strcmp("delete",tokens[0]) == 0){
    //         printf("delete function for key %s executed\n",tokens[1]);
    //         delete(atoi(tokens[1]), newsockfd);
    //     }
    //     else if(strcmp("disconnect",tokens[0]) == 0){
    //         if(close(newsockfd) == 0){
    //             printf("\nConnection terminated\n");
    //             break;
    //         }
    //     }
    // }
    // return 0;

    
}

void enqueue(int *queue, int socket){
    pthread_mutex_lock(&lock);

    // here handle the case where if the connections max out the queue (use while loop) hint: make the server sleep for a few amt of time if the connections max out

    while(elements_count == MAX_THREADS){
        pthread_mutex_unlock(&lock);
        sleep(5);
        pthread_mutex_lock(&lock);
    }

    // socket being added to the queue

    queue[rear] = socket;

    rear = (rear+1)%MAX_THREADS;
    elements_count++;

    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&lock);
}

int dequeue(int *queue){
    pthread_mutex_lock(&lock);

    while(elements_count == 0){
        pthread_cond_wait(&cond,&lock);
    }

    int client_sock = queue[front];
    front = (front+1)%MAX_THREADS;
    elements_count--;

    pthread_mutex_unlock(&lock);
    return client_sock;
}

void error(char *msg)
{
    perror(msg);
    exit(1);
}

bool search(struct node *head, int val){
    if(head == NULL)return false;
    struct node *temp = head;
    while(temp != NULL){
        if(temp -> key == val)return true;
        temp = temp -> next;
    }
    return false;
}

struct node *find_node(int val, int *found) {
    if(head == NULL)return NULL;
    struct node *temp = head, *bin = NULL;
    while(temp != NULL){
        if(temp -> key == val){
            *found = 1;
            break;
        }
        bin = temp;
        temp = temp -> next;
    }
    return bin;
}


void create(int key, int value_size,int newsockfd){
    
    // key not found

    struct node *temp = NULL;
    if(!search(head,key)){

        // send the message to the server that we're good to recieve the data

        n2 = write(newsockfd,"1",1);
        if (n2 < 0) error("ERROR writing to socket");

        // allocate memory for data

        char *data = malloc(sizeof(char) * (value_size + 1));
        int data_ptr = 0,buffer_ptr = 0;
        char ch;
        while(value_size > 0){

            // recieveing the message
            
            bzero(buffer,256);
            n1 = read(newsockfd,buffer,255);
            if (n1 < 0) error("ERROR reading from socket");
            // printf("Here is the message: %s\n",buffer);

            // adding the message to the data

            buffer_ptr = 0;
            while(buffer[buffer_ptr] != '\0')data[data_ptr++] = buffer[buffer_ptr++];
            value_size = value_size - buffer_ptr;

        }
        data[data_ptr] = '\0';
        // printf("Here is the message: %s\n",data);

        // creating the node

        struct node *ptr = (struct node *)malloc(sizeof(struct node));
        ptr -> key = key;
        ptr -> data = data;
        ptr -> next = NULL;

        if(head == NULL) head = ptr;
        else{
            ptr -> next = head;
            head = ptr;
        }

        bzero(buffer,256);
        buffer[0] = 'O';
        buffer[1] = 'K';
        buffer[2] = '\0';
        n2 = write(newsockfd,buffer,3);
        if (n2 < 0) error("ERROR writing to socket");
    }
    
    // key found

    else{
        // send the message to server that can't create a new key
        n2 = write(newsockfd,"0",1);
        if (n2 < 0) error("ERROR writing to socket");
    }

}

void update(int key, int value_size, int newsockfd){
    
    // key found
    found = 0;
    struct node *temp = find_node(key,&found);

    if(found == 1){

        // send the message to the server that we're good to recieve the data

        n2 = write(newsockfd,"1",1);
        if (n2 < 0) error("ERROR writing to socket");
        

        // allocate memory for data

        char *data = malloc(sizeof(char) * (value_size + 1));
        int data_ptr = 0,buffer_ptr = 0;
        char ch;
        while(value_size > 0){

            // recieveing the message
            
            bzero(buffer,256);
            n1 = read(newsockfd,buffer,255);
            if (n1 < 0) error("ERROR reading from socket");
            // printf("Here is the message: %s\n",buffer);

            // adding the message to the data

            buffer_ptr = 0;
            while(buffer[buffer_ptr] != '\0')data[data_ptr++] = buffer[buffer_ptr++];
            value_size = value_size - buffer_ptr;

        }
        data[data_ptr] = '\0';
        // printf("Here is the message: %s\n",data);

        // updating the data

        if (head -> key == key){

            free(head -> data);
            head -> data = data;

            bzero(buffer,256);
            buffer[0] = 'O';
            buffer[1] = 'K';
            buffer[2] = '\0';
            n2 = write(newsockfd,buffer,3);
                if (n2 < 0) error("ERROR writing to socket");
        }
        else{

            free(temp -> next -> data);
            temp -> next -> data = data;

            bzero(buffer,256);
            buffer[0] = 'O';
            buffer[1] = 'K';
            buffer[2] = '\0';
            n2 = write(newsockfd,buffer,3);
                if (n2 < 0) error("ERROR writing to socket");
        }
    }
    
    // key not found

    else{
        // send the message to server that can't create a new key
        n2 = write(newsockfd,"0",1);
        if (n2 < 0) error("ERROR writing to socket");
    }

}

void reads(int key, int newsockfd){

    // key found

    found = 0;
    struct node *temp = find_node(key,&found);

    if(found == 1){

        // workaround fix for temp

        if (head -> key == key){
            temp = head;
        }
        else temp = temp->next;

        // send the message that key found

        n2 = write(newsockfd,"1",1);
        if (n2 < 0) error("ERROR writing to socket");
        
        // metadata is being sent

        bzero(buffer,256);
        char *temp1 = temp->data;
        int val_size = 0;

        // printf("%s",temp1);
        while(temp1[val_size] != '\0')++val_size;
        snprintf(buffer, 255, "%d", val_size);
        // printf("%d",val_size);
        // printf("\n\n%s\n",buffer);

        // value is being sent

        n2 = write(newsockfd,buffer,16);
        if (n2 < 0) error("ERROR writing to socket");

        int sent = 0;                 // bytes already sent
        int remaining = val_size;     // total bytes left
        const char *data = temp1;     // pointer to the string you want to send
        
        while (remaining > 0) {
            // calculate current chunk size
            int chunk_size = (remaining >= 255) ? 255 : remaining;
        
            // clear the buffer
            bzero(buffer, 256);
        
            // manually copy 255 bytes (or less) from data into buffer
            int i = 0;
            while (i < chunk_size) {
                buffer[i] = data[sent + i];
                i++;
            }
        
            // null terminate for safety (not required for write)
            buffer[i] = '\0';
        
            // send this chunk to client
            n2 = write(newsockfd, buffer, chunk_size);
            if (n2 < 0)
                error("ERROR writing to socket");
        
            // optional debug
            // printf("Sent chunk (%d bytes): %.*s\n", chunk_size, chunk_size, buffer);
        
            // update counters
            sent += chunk_size;
            remaining -= chunk_size;
        }
    }
    else{

        // send the message that key not found

        n2 = write(newsockfd,"0",1);
        if (n2 < 0) error("ERROR writing to socket");
    }
}

void delete(int key, int newsockfd){
    
    // key found
    
    found = 0;
    struct node *temp = find_node(key,&found);

    if(found == 1){

        // delete the key

        if (head -> key == key){
            temp = head->next;
            free(head->data);
            free(head);
            head = temp;
        }
        else{
            struct node *to_delete = temp->next;
            temp = temp->next->next;
            free(to_delete->data);
            free(to_delete);
        }
        
        // send the message to the server that we found the data

        n2 = write(newsockfd,"1",1);
        if (n2 < 0) error("ERROR writing to socket");
        
    }
    else{
        // send the message to server that can't create a new key

        n2 = write(newsockfd,"0",1);
        if (n2 < 0) error("ERROR writing to socket");
    }
}