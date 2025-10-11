#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#include <strings.h>
#include <stdlib.h>

#define MAX_TOKENS 4

// declaration of variables

char buffer[256];
char *tokens[MAX_TOKENS];
int size_of_string;
char *line = NULL;
size_t len = 0;
int count;
int sockfd, portno, n;
struct sockaddr_in serv_addr;
struct hostent *server;
int conn_established = 0;

// declaring functions

void error(char *msg);
void create();
void update();
void reads();
void delete();
void client_calls(char *buffer);
int tokenizer(char *line);
int count_length(int filefd);

// main function

int main(int argc, char *argv[]){

	//declaration of variables

	if(!(argc < 4))exit(0);

	//interactive mode

	if(argc == 2 && strcmp(argv[1],"interactive") == 0){
		
		while(1){

			printf("\nEnter the options : ");
			size_of_string = getline(&line, &len, stdin);
			if(size_of_string == -1){
				perror("getline failed");
				exit(1);
			}

			if(line[size_of_string-1] == '\n') {
        		line[size_of_string-1] = '\0';
   			}

   			// // print all tokens
    		// printf("\nTokens found (%d):\n", token_count);

    		// for (int j = 0; j < token_count; j++) {
        	// 	printf("Token %d: %s\n", j + 1, tokens[j]);
    		// }

			client_calls(line);

		}
	}

	//batch mode (this is currently WIP)

	else if (argc == 3 && strcmp(argv[1], "batch") == 0) {
        int filefd = open(argv[2], O_RDONLY);
        if (filefd < 0) {
            perror("Error opening file");
            exit(1);
        }

        ssize_t ns;
        char ch;
        off_t curr_pos;

        // Read loop — continues until EOF
        while ((ns = read(filefd, &ch, 1)) > 0) {
            // Move one character back so we can process this line from the start
            lseek(filefd, -1, SEEK_CUR);

            // Remember where we are
            curr_pos = lseek(filefd, 0, SEEK_CUR);

            // Count length of current line
            int len = count_length(filefd);

            // Reset file pointer to the beginning of this line
            lseek(filefd, curr_pos, SEEK_SET);

            // Allocate memory (+1 for '\0')
            char *line = (char *)malloc(len + 1);
            if (!line) {
                perror("malloc failed");
                break;
            }

            // Read full line using for loop (excluding newline)
            for (int j = 0; j < len; j++) {
                read(filefd, &ch, 1);
                line[j] = ch;
            }

            // Read and discard newline (if present)
            read(filefd, &ch, 1);  // This consumes '\n' or EOF

            line[len] = '\0'; // Only null-terminate, no '\n'

            //printf("%s\n", line); // We can add our own newline if desired
            
            client_calls(line);

            free(line);
        }

        close(filefd);
    } else {
        printf("Usage: %s batch <filename>\n", argv[0]);
    }

    return 0;
}

// other functions

void error(char *msg)
{
    perror(msg);
    exit(0);
}

int count_length(int filefd) {
    char ch;
    int length = 0;
    ssize_t ns;

    // Read until newline or EOF
    while ((ns = read(filefd, &ch, 1)) > 0 && ch != '\n')length++;

    return length;
}

void create(){

	//metadata is being sent

	char *temp;
	int cnt = 0,i ,bytes_to_write = atoi(tokens[2]);

	for(int j = 0; j < count - 1 ; j++){
		temp = tokens[j];
		i = 0;
		while(temp[i] != '\0'){
			buffer[cnt++] = temp[i++];
		}
		buffer[cnt++] = ' ';
	}
	
	n = write(sockfd, buffer, strlen(buffer));
	if (n < 0) error("ERROR writing to socket");
	cnt = 0;

	//read to confirm wheather we need to send the data or not

	bzero(buffer,256);

	n = read(sockfd,buffer,255);
    if (n < 0) 
		error("ERROR reading from socket");

	// 1 = send the data, 0 = don't send the data

    if(buffer[0] == '1'){

    	// Send value to server chunks

		char *temp = tokens[count - 1];  // data to send
		int total_len = strlen(temp);
		int bytes_sent = 0;
		
		while (bytes_sent < total_len) {

			// clear buffer

		    bzero(buffer, 256);  
		
		    // Determine chunk size
		    
		    int chunk_size = (total_len - bytes_sent >= 255) ? 255 : (total_len - bytes_sent);
		
		    // copy characters into buffer
		    
		    for (int i = 0; i < chunk_size; i++) {
		        buffer[i] = temp[bytes_sent + i];
		    }
		    buffer[chunk_size] = '\0';  
		
		    // Send the chunk
		    
		    n = write(sockfd, buffer, chunk_size);
		    if (n < 0)
		        error("ERROR writing to socket");
			
			// move to next block
		    
		    bytes_sent += chunk_size;  
		}
		
		// Read final ACK from server
		
		bzero(buffer, 256);
		n = read(sockfd, buffer, 255);
		if (n < 0)
		    error("ERROR reading from socket");
		
    }

    // display the message that key already exists

    else{
    	printf("The key %d already exists\n", atoi(tokens[1]));
    }
}

void update(){

	//metadata is being sent

	char *temp;
	int cnt = 0,i ,bytes_to_write = atoi(tokens[2]);

	for(int j = 0; j < count - 1 ; j++){
		temp = tokens[j];
		i = 0;
		while(temp[i] != '\0'){
			buffer[cnt++] = temp[i++];
		}
		buffer[cnt++] = ' ';
	}
	
	n = write(sockfd, buffer, strlen(buffer));
	if (n < 0) error("ERROR writing to socket");
	cnt = 0;

	//read to confirm wheather we need to send the data or not

	bzero(buffer,256);

	n = read(sockfd,buffer,255);
    if (n < 0) 
		error("ERROR reading from socket");

	// 1 = send the data, 0 = don't send the data

    if(buffer[0] == '1'){

    	// Send value to server chunks

		char *temp = tokens[count - 1];  // data to send
		int total_len = bytes_to_write;  // strlen(temp);
		int bytes_sent = 0;
		
		while (bytes_sent < total_len) {

			// clear buffer

		    bzero(buffer, 256);  
		
		    // Determine chunk size
		    
		    int chunk_size = (total_len - bytes_sent >= 255) ? 255 : (total_len - bytes_sent);
		
		    // copy characters into buffer
		    
		    for (int i = 0; i < chunk_size; i++) {
		        buffer[i] = temp[bytes_sent + i];
		    }
		    buffer[chunk_size] = '\0';  
		
		    // Send the chunk
		    
		    n = write(sockfd, buffer, chunk_size);
		    if (n < 0)
		        error("ERROR writing to socket");
			
			// move to next block
		    
		    bytes_sent += chunk_size;  
		}

		printf("bytes sent %d\n", bytes_sent);
		
		// Read final ACK from server
		
		bzero(buffer, 256);
		n = read(sockfd, buffer, 255);
		if (n < 0)
		    error("ERROR reading from socket");

    }

    // display the message that key already exists

    else{
    	printf("The key %d doesn't exists\n", atoi(tokens[1]));
    }
}

void reads(){

	//metadata is being sent

	char *temp;
	int cnt = 0,i ;

	for(int j = 0; j < count ; j++){
		temp = tokens[j];
		i = 0;
		while(temp[i] != '\0'){
			buffer[cnt++] = temp[i++];
		}
		buffer[cnt++] = ' ';
	}
	
	n = write(sockfd, buffer, strlen(buffer));
	if (n < 0) error("ERROR writing to socket");
	cnt = 0;

	// read to confirm wheather we need to receive the data or not

	bzero(buffer,256);

	n = read(sockfd,buffer,1);
    if (n < 0) 
		error("ERROR reading from socket");

	if(buffer[0] == '1'){

		// getting the value size
    	
		bzero(buffer,256);

		n = read(sockfd,buffer,16);
   		if (n < 0) 
			error("ERROR reading from socket");

		int value_size = atoi(buffer);
		bzero(buffer,256);

		// allocate buffer for the data (value_size is from server)

		char *data = malloc(value_size + 1);
		if (!data) error("Memory allocation failed");

		// receive the data
		
		int received = 0;
		
		while (received < value_size) {
		    // read up to 255 bytes at a time
		    n = read(sockfd, buffer, 255);
		    if (n < 0)
		        error("ERROR reading from socket");
		    else if (n == 0)
		        break;  // connection closed prematurely
		
		    // append the message manually (no memcpy)
		    int buffer_ptr = 0;
		    while (buffer_ptr < n) {  // use n bytes actually read
		        data[received++] = buffer[buffer_ptr++];
		    }
		
		    // optional debug info
		    printf("Received chunk (%d bytes), total = %d\n", n, received);
		}
		
		// null terminate the full message
		data[received] = '\0';
		
		printf("\ndata : %s\n", data);
		free(data);
    }

    // display the message that key doesn't exists

    else{
    	printf("The key %d doesn't exists\n", atoi(tokens[1]));
    }

}

void delete(){

	//metadata is being sent

	char *temp;
	int cnt = 0,i ;

	for(int j = 0; j < count ; j++){
		temp = tokens[j];
		i = 0;
		while(temp[i] != '\0'){
			buffer[cnt++] = temp[i++];
		}
		buffer[cnt++] = ' ';
	}
	
	n = write(sockfd, buffer, strlen(buffer));
	if (n < 0) error("ERROR writing to socket");
	cnt = 0;

	//read to confirm wheather we need to send the data or not

	bzero(buffer,256);

	n = read(sockfd,buffer,255);
    if (n < 0) 
		error("ERROR reading from socket");

	// if reply is 1 then successfully deleted

	if(buffer[0] == '1')printf("the key %d is deleted\n", atoi(tokens[1]));

    // display the message that key doesn't exists

    else{
    	printf("The key %d doesn't exists\n", atoi(tokens[1]));
    }

}	

void client_calls(char *buffer){
	
	//remove the end \n
	
	buffer[strcspn(buffer, "\n")] = '\0';

	//tokenize the input

	count = tokenizer(buffer);

	// char *token = strtok(buffer," ");
	// while(token != NULL && count < MAX_TOKENS){
	// 	tokens[count++] = token;
	// 	// printf("%s\n",token);
	// 	token = strtok(NULL," ");
	// }

	//connect disconnect

	if(strcmp(tokens[0],"connect") == 0){
		if(count != 3){
			printf("Please enter in this format 'connect <server-ip> <server-port>'\n");
		}

		else{

			if(conn_established == 1){
				printf("\nconnection is already established to 1 server\n");
			}
			else{
				// creating a socket

				sockfd = socket(AF_INET, SOCK_STREAM, 0);
				portno = atoi(tokens[2]);
				if (sockfd < 0) error("ERROR opening socket");

				//server details which needs to be filled in
				
				server = gethostbyname(tokens[1]);
				if (server == NULL) {
    				fprintf(stderr,"ERROR, no such host\n");
    				exit(0);
    			}
    			bzero((char *) &serv_addr, sizeof(serv_addr));
				serv_addr.sin_family = AF_INET;
				bcopy((char *)server->h_addr, 
 					(char *)&serv_addr.sin_addr.s_addr,
 					server->h_length);
				serv_addr.sin_port = htons(portno);

				//connecting to the server

				if(connect(sockfd,(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0){
					error("ERROR connecting");
				}
				else{
					printf("\nOK\n");
					conn_established = 1;
				}
			}         
		}
	}

	else if(strcmp(tokens[0],"disconnect") == 0){

		if(conn_established == 0){
			printf("No active connection to the server\n");
		}
		else{

			n = write(sockfd,"disconnect",11);
			if (n < 0) 
				error("ERROR reading from socket");

			if(close(sockfd) == 0){
				printf("\nOK\n");
				conn_established = 0;
			}
			else error("ERROR closing the socket");
		}
			
	}

	//CRUD operations

	else if((strcmp(tokens[0],"create") == 0) || (strcmp(tokens[0],"read") == 0) || (strcmp(tokens[0],"update") == 0) || (strcmp(tokens[0],"delete") == 0)){
		if(conn_established == 0){
			printf("No active connection to the server\n");
		}
		else{
			if(strcmp(tokens[0],"create") == 0){
				if(count == 4){
					create();
				}
				else{
					printf("Please enter in this format 'create <key> <value-size> <value>'\n");
				}
			}
			else if(strcmp(tokens[0],"read") == 0){
				if(count == 2){
					reads();
				}
				else{
					printf("Please enter in this format 'read <key>'\n");
				}
			}
			else if(strcmp(tokens[0],"update") == 0){
				if(count == 4){
					update();
				}
				else{
					printf("Please enter in this format 'update <key> <value-size> <value>'\n");
				}
			}
			else{
				if(count == 2){
					delete();
				}
				else{
					printf("Please enter in this format 'delete <key>'\n");
				}
			}
		}	
	}

	else printf("Incorrect Input\n");
	count = 0;
}

int tokenizer(char *line){
    int i = 0,token_count=0;
    int max_tokens = MAX_TOKENS;
    while (line[i] != '\0' && token_count < max_tokens - 1) { 
        // skip spaces
        while (line[i] == ' ') i++;
        if (line[i] == '\0') break;

        // mark start of token
        tokens[token_count++] = &line[i];

        // move until space or end
        while (line[i] != ' ' && line[i] != '\0') i++;

        // terminate token if space found
        if (line[i] == ' ') {
            line[i] = '\0';
            i++;
        }
        if(token_count == 1){
            if(strcmp("read",tokens[0]) == 0)max_tokens = 3;
            else if(strcmp("delete",tokens[0]) == 0)max_tokens = 3;
            else if(strcmp("disconnect",tokens[0]) == 0)max_tokens = 2;   
        }
    }

    // if there's still text left, it becomes the 4th token
    if (line[i] != '\0') {
        tokens[token_count++] = &line[i];
    }

    return token_count;
}