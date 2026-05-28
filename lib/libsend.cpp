#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <string.h>

using namespace std;

#define DATA 1
#define SYN 2
#define SYN_ACK 3 
#define ACK 4

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int max_window_pkts = MAX_NUMBER_PKTS;
pthread_mutex_t poll_lock = PTHREAD_MUTEX_INITIALIZER;

int send_data(int conn_id, char *buffer, int len)
{
    if (len > MAX_DATA_SIZE) {
        len = MAX_DATA_SIZE;
    }

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    while (true) {
        int16_t pachete_tranzit = (int16_t)(cons[conn_id]->next_seq - cons[conn_id]->current_seq);
        
        if (pachete_tranzit < max_window_pkts) {
            break;
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(100);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    uint16_t seq = cons[conn_id]->next_seq;
    int index = seq % MAX_NUMBER_PKTS;

    struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)cons[conn_id]->send_window[index].pkt.data;
    hdr->conn_id = conn_id;
    hdr->len = len;
    hdr->protocol_id = POLI_PROTOCOL_ID;
    hdr->seq_num = seq;
    hdr->type = DATA;

    memcpy(cons[conn_id]->send_window[index].pkt.data + sizeof(struct poli_tcp_data_hdr), buffer, len);

    cons[conn_id]->send_window[index].pkt.len = sizeof(struct poli_tcp_data_hdr) + len;
    cons[conn_id]->send_window[index].seq_num = seq;
    cons[conn_id]->send_window[index].is_occupied = true;
    cons[conn_id]->send_window[index].is_sent = true;

    sendto(cons[conn_id]->sockfd, cons[conn_id]->send_window[index].pkt.data, 
            cons[conn_id]->send_window[index].pkt.len, 0, 
            (struct sockaddr*)&cons[conn_id]->servaddr, sizeof(cons[conn_id]->servaddr));

    cons[conn_id]->next_seq++;

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return len;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {
        if (cons.size() == 0) {
            usleep(100000);
            continue;
        }

        for (int i = 0; i < MAX_CONNECTIONS; i++) {
            if (cons.count(i) == 0) continue;
            struct connection *con = cons[i];
            if (con == NULL) continue;
            
            pthread_mutex_lock(&con->con_lock);
            
            for(uint16_t s = con->current_seq; s != con->next_seq; s++) {
                int idx = s % MAX_NUMBER_PKTS;
                if(con->send_window[idx].is_occupied && !con->send_window[idx].is_sent) {
                    sendto(con->sockfd, con->send_window[idx].pkt.data, 
                            con->send_window[idx].pkt.len, 0, 
                            (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
                    con->send_window[idx].is_sent = true; 
                }
            }
            pthread_mutex_unlock(&con->con_lock);
        }

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        if (res == -1) {
            for (int i = 0; i < MAX_CONNECTIONS; i++) {
                if (cons.count(i) == 0) continue;
                struct connection *con = cons[i];
                if (con == NULL) continue;
                
                pthread_mutex_lock(&con->con_lock);
                int count = 0;
                for(uint16_t s = con->current_seq; s != con->next_seq && count < 4; s++) {
                    int idx = s % MAX_NUMBER_PKTS;
                    if (con->send_window[idx].is_occupied) {
                         sendto(con->sockfd, con->send_window[idx].pkt.data, 
                                con->send_window[idx].pkt.len, 0, 
                                (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
                         count++;
                    }
                }
                pthread_mutex_unlock(&con->con_lock);
            }
            continue;
        }

        if (conn_id == -1 || res <= 0) continue; 
        if (cons.count(conn_id) == 0 || cons[conn_id] == NULL) continue;

        pthread_mutex_lock(&cons[conn_id]->con_lock);
        struct poli_tcp_ctrl_hdr *ack_hdr = (struct poli_tcp_ctrl_hdr *)buf;
        
        if (ack_hdr->type == ACK) {
            uint16_t ack_num = ack_hdr->ack_num;
            cons[conn_id]->recv_window_size = ack_hdr->recv_window;
            int16_t diff = (int16_t)(ack_num - cons[conn_id]->current_seq);
            
            if (diff > 0) {
                while (cons[conn_id]->current_seq != ack_num) {
                    int idx = cons[conn_id]->current_seq % MAX_NUMBER_PKTS;
                    cons[conn_id]->send_window[idx].is_occupied = false;
                    cons[conn_id]->send_window[idx].is_sent = false;
                    cons[conn_id]->current_seq++; 
                }
            }
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    struct connection *con = (struct connection *)calloc(1, sizeof(struct connection));
    pthread_mutex_init(&con->con_lock, NULL);

    con->recv_window_size = MAX_SIZE_BUFFER;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    struct sockaddr_in zero_addr = {0};
    con->servaddr = zero_addr;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_port = port;

    struct poli_tcp_ctrl_hdr syn_pe_port = {0};
    syn_pe_port.protocol_id = POLI_PROTOCOL_ID;
    syn_pe_port.type = SYN;
    syn_pe_port.conn_id = 0; // Temporar

    struct timeval tv_handshake = {1, 0};
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv_handshake, sizeof(tv_handshake)) < 0) {
        perror("Eroare: timeout la handshake!!!");
    }

    int bytes_recv;
    char buffer_client[1500] = {0};
    struct sockaddr_in ack = {0};
    socklen_t len = sizeof(ack);

    while (true) {
        printf("Se trimite SYN catre server...\n");
        sendto(con->sockfd, &syn_pe_port, sizeof(syn_pe_port), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
        printf("Se asteapta SYN-ACK de la server...\n");
        bytes_recv = recvfrom(con->sockfd, buffer_client, sizeof(buffer_client), 0, (struct sockaddr*)&ack, &len);
        if (bytes_recv >= 0) break; 
        printf("Aparent SYN s-a pierdut pe retea, retrimitere pachet...\n");
    }

    struct poli_tcp_data_hdr *ack_hdr = (struct poli_tcp_data_hdr *)buffer_client;
    
    // FIX COMPILARE: Fara "int" ca era redeclarat!
    int conn_id = ack_hdr->conn_id;
    con->conn_id = conn_id;

    if (ack_hdr->type == SYN_ACK) {
        uint16_t new_port = *(uint16_t*)(buffer_client + sizeof(struct poli_tcp_data_hdr));
        printf("SYN-ACK primit cu succes! conn_id=%d\n", conn_id);
        con->servaddr.sin_port = new_port;
    } else {
        printf("EROARE: Raspunsul nu este un syn-ack\n");
        free(con);
        return -1;
    }

    struct poli_tcp_ctrl_hdr ack_primire_packet = {0};
    ack_primire_packet.protocol_id = POLI_PROTOCOL_ID;
    ack_primire_packet.type = ACK;
    ack_primire_packet.conn_id = conn_id;

    printf("Se trimite ack catre server...\n");
    for (int k = 0; k < 5; k++) {
        sendto(con->sockfd, &ack_primire_packet, sizeof(ack_primire_packet), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
        usleep(2000); 
    }

    struct timeval tv = {0, 0};
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Error");
    }

    pthread_mutex_lock(&poll_lock);
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 10000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 10000000; 
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);  
    fdmax++;
    cons.insert({conn_id, con});
    pthread_mutex_unlock(&poll_lock);

    DEBUG_PRINT("Connection established!");
    return conn_id;
}

void init_sender(int speed, int delay)
{
    max_window_pkts = 150;
    if (max_window_pkts < 10) max_window_pkts = 10;
    if (max_window_pkts > MAX_NUMBER_PKTS) max_window_pkts = MAX_NUMBER_PKTS;

    printf("Numar pachete pe fir: %d\n", max_window_pkts);
    pthread_t thread1;
    int ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}