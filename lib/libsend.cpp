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
#define ACK 4// in cerinta se specifica faptul ca pot alege orice valori

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
// fereastra cu BDP
int max_window_pkts = MAX_NUMBER_PKTS;
pthread_mutex_t poll_lock = PTHREAD_MUTEX_INITIALIZER;

int send_data(int conn_id, char *buffer, int len)
{
    /* We will write code here as to not have sync problems with sender_handler */

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    uint16_t pachete_tranzit = cons[conn_id]->next_seq - cons[conn_id]->current_seq;

    while (pachete_tranzit >= max_window_pkts || cons[conn_id]->recv_window_size < len) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
        
        // recalculez in cazul in care s-a eliberat un loc in buffer-ul meu
        pachete_tranzit = cons[conn_id]->next_seq - cons[conn_id]->current_seq;
    }

    // calculul pozitiei unde o sa vina pachetul de pe teava
    uint16_t seq = cons[conn_id]->next_seq;
    int index = seq % MAX_NUMBER_PKTS;

    // header pentru pozitia din buffer a pachetului
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

    /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

    while (1) {

        if (cons.size() == 0) {
            usleep(100000);
            continue;
        }

        // trimitere pachete noi din buffer
        for (int i = 0; i < MAX_CONNECTIONS; i++) {
            if (cons.count(i) == 0) {
                continue;
            } 

            struct connection *con = cons[i];
            if (con == NULL) {
                continue;
            }
            
            pthread_mutex_lock(&con->con_lock);
            
            for(uint16_t s = con->current_seq; s != con->next_seq; s++) {
                int idx = s % MAX_NUMBER_PKTS;
                
                // Daca e in fereastra si nu l-am trimis inca
                if(con->send_window[idx].is_occupied && !con->send_window[idx].is_sent) {
                    sendto(con->sockfd, con->send_window[idx].pkt.data, 
                            con->send_window[idx].pkt.len, 0, 
                            (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
                     
                    // Marchez ca a plecat pe teava
                    con->send_window[idx].is_sent = true; 
                }
            }
            
            pthread_mutex_unlock(&con->con_lock);
        }

        // asteapta un eveniment de la server
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
                uint16_t base = con->current_seq;
                int idx = base % MAX_NUMBER_PKTS;
                
                if (con->send_window[idx].is_occupied) {
                     sendto(con->sockfd, con->send_window[idx].pkt.data, 
                            con->send_window[idx].pkt.len, 0, 
                            (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
                }
                pthread_mutex_unlock(&con->con_lock);
            }
            continue;
        }

        if (conn_id == -1 || res <= 0) {
            continue; // daca id-ul este invalid se trece la urmatorul pachet
        }

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        // qm primit ceva de la server
        struct poli_tcp_ctrl_hdr *ack_hdr = (struct poli_tcp_ctrl_hdr *)buf;
        
        if (ack_hdr->type == ACK) {
            uint16_t ack_num = ack_hdr->ack_num;
            
            cons[conn_id]->recv_window_size = ack_hdr->recv_window;
            
            while (cons[conn_id]->current_seq != ack_num) {
                int idx = cons[conn_id]->current_seq % MAX_NUMBER_PKTS;
                cons[conn_id]->send_window[idx].is_occupied = false;
                cons[conn_id]->send_window[idx].is_sent = false;
                
                cons[conn_id]->current_seq++; // se muta fereastra glisanta la dreapta
            }
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)calloc(1, sizeof(struct connection));
    //int conn_id = 0;
    int conn_id = cons.size(); // id unic pentru fiecare conexiune ca altfel crapa checkerul
    con->recv_window_size = MAX_SIZE_BUFFER;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    // se seteaza adresa initiala a portului
    struct sockaddr_in zero_addr = {0};
    con->servaddr = zero_addr;

    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_port = port;

    // trimitere pachet syn pe portul deschis
    struct poli_tcp_ctrl_hdr syn_pe_port = {0};
    syn_pe_port.protocol_id = POLI_PROTOCOL_ID;
    syn_pe_port.type = SYN;
    syn_pe_port.conn_id = conn_id;

    // timeout pentru handshake
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

        if (bytes_recv >= 0) {
            break; // daca se primeste raspuns de la server se iese din bucla
        }
        
        printf("Aparent SYN s-a pierdut pe retea, retrimitere pachet...\n");
    }

    struct poli_tcp_data_hdr *ack_hdr = (struct poli_tcp_data_hdr *)buffer_client;
    
    if (ack_hdr->type == SYN_ACK) {
        uint16_t new_port = *(uint16_t*)(buffer_client + sizeof(struct poli_tcp_data_hdr));
        conn_id = ack_hdr->conn_id;
        con->conn_id = conn_id;
        printf("Ack s-a primit cu succes!\n");
        
        con->servaddr.sin_port = new_port;
    } else {
        printf("EROARE: Raspunsul nu este un syn-ack\n");
    }

    // trimitere ack ca s-a primit ack de la server

    struct poli_tcp_ctrl_hdr ack_primire_packet = {0};

    ack_primire_packet.protocol_id = POLI_PROTOCOL_ID;
    ack_primire_packet.type = ACK;
    ack_primire_packet.conn_id = conn_id;

    printf("Se trimite ack catre server...\n");
    sendto(con->sockfd, &ack_primire_packet, sizeof(ack_primire_packet), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));

    struct timeval tv = {0}; 
    tv.tv_sec = 2;   // 2 secunde
    tv.tv_usec = 0; // 0 microsecunde
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Error");
    }

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */

    pthread_mutex_lock(&poll_lock);

    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 200000000;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = 200000000;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);  
    fdmax++;

    pthread_mutex_unlock(&poll_lock);

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    double rtt_sec = (2.0 * delay) / 1000.0;
    double bdp = (speed * 1024.0 * 1024.0 / 8.0) * rtt_sec;
    max_window_pkts = bdp / MAX_SEGMENT_SIZE;

    if (max_window_pkts < 10) {
        max_window_pkts = 10;
    }
    if (max_window_pkts > MAX_NUMBER_PKTS) {
        max_window_pkts = MAX_NUMBER_PKTS;
    }

    printf("Numar pachete pe fir: %d\n", max_window_pkts);
    
    pthread_t thread1;
    int ret;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
