#include <stdio.h>
#include <sys/types.h> 
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>

#define MAX_TOKENS 3

//declaring variables

int sockfd, newsockfd, portno; 
socklen_t clilen;
struct sockaddr_in serv_addr, cli_addr;
int n1, n2;
int packet_1st = 0;
char *tokens[MAX_TOKENS];
char buffer[256];
int count,found;

struct node{
    char *data;
    int key;
    struct node *next;
};

// linked list for storing data

struct node *head = NULL;

// declaring functions

void error(char *msg);
bool search(struct node *head, int val);
struct node *find_node(int val, int *found);
void create(int key, int value_size);
void update(int key, int value_size);
void reads(int key);
void delete(int key);
// disconnect function is yet to be implemented

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

    if(listen(sockfd,1) < 0)error("ERROR listening");
    clilen = sizeof(cli_addr); 

    // handling new connections

    while(1){
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) 
             error("ERROR on accept");


        while(1 /*connection established per handle */ ){
            
            // read meta data packet from the server

            n1 = read(newsockfd,buffer,255);
                if (n1 < 0) error("ERROR reading from socket");
                // packet_1st = 1;

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
                create(atoi(tokens[1]),atoi(tokens[2]));
                printf("create function for key %s executed\n",tokens[1]);
            }
            else if(strcmp("update",tokens[0]) == 0){
                update(atoi(tokens[1]),atoi(tokens[2]));
                printf("update function for key %s executed\n",tokens[1]);
            }
            else if(strcmp("read",tokens[0]) == 0){
                reads(atoi(tokens[1]));
                printf("read function for key %s executed\n",tokens[1]);
            }
            else if(strcmp("delete",tokens[0]) == 0){
                delete(atoi(tokens[1]));
                printf("delete function for key %s executed\n",tokens[1]);
            }
            else if(strcmp("disconnect",tokens[0]) == 0){
                if(close(newsockfd) == 0){
                    printf("\nConnection terminated\n");
                    break;
                }
            }
        }
    }
}

// other functions

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


void create(int key, int value_size){
    
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

void update(int key, int value_size){
    
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

void reads(int key){

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

void delete(int key){
    
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