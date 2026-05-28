
#pragma once // directiva care ii spune procesorului sa il includa o singura data la compilare
             // indiferent de cate ori este intalnit

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>

/* Maximum segment size, change as you see fit */
//#define MAX_DATA_SIZE 512
#define MAX_DATA_SIZE 1024
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

#define MAX_NUMBER_PKTS 1000
#define MAX_SIZE_BUFFER 200000

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */

 struct packet_info {
    char data[1500];
    int len;
};

struct window_slot {
    uint16_t seq_num; // numarul pachetului
    bool is_occupied; // flag daca exista pachet
    bool is_sent;
    struct packet_info pkt; // pachetul in sine
};

struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    int app_buffer_start;
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    /* TODO. Parameters used only by the sender */
    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */
    uint16_t current_seq;           
    uint16_t next_seq;

    struct window_slot send_window[MAX_NUMBER_PKTS];

    /* TODO. Parameters used only by the server */
    uint16_t expected_seq;       
    int recv_window_size;        
    
    struct window_slot out_of_order_window[MAX_NUMBER_PKTS];

    char app_buffer[MAX_SIZE_BUFFER]; 
    int app_buffer_len;
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
