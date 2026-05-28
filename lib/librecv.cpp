#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
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

pthread_mutex_t poll_lock = PTHREAD_MUTEX_INITIALIZER;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */

    // daca nu am date primite, astept sa vina
    while (cons[conn_id]->app_buffer_len == 0) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000); // trebuie sa las putin dupa unlock sa mai stea ca sa poata primi date de pe retea
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    // cate date se pot scoate din buffer
    if (len < cons[conn_id]->app_buffer_len) {
        size = len;
    } else {
        size = cons[conn_id]->app_buffer_len;
    }

    // copiez datele in fisierul de out
    memcpy(buffer, cons[conn_id]->app_buffer + cons[conn_id]->app_buffer_start, size);

    cons[conn_id]->app_buffer_start += size;
    cons[conn_id]->app_buffer_len -= size;

    if (cons[conn_id]->app_buffer_len == 0) {
        cons[conn_id]->app_buffer_start = 0;
    }

    cons[conn_id]->recv_window_size = MAX_SIZE_BUFFER - cons[conn_id]->app_buffer_len;

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void send_ack(int connection_id, uint16_t ack_num) 
{
    struct poli_tcp_ctrl_hdr ack = {0};
    ack.ack_num = ack_num;
    ack.conn_id = connection_id;
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.type = ACK;
    ack.recv_window = cons[connection_id]->recv_window_size;

    sendto(cons[connection_id]->sockfd, &ack, sizeof(ack), 0, (struct sockaddr*)&cons[connection_id]->servaddr, sizeof(cons[connection_id]->servaddr));
}

int save_in_buffer(int connection_id, char *payload, int payload_len) 
{
    int write_pos = cons[connection_id]->app_buffer_start + cons[connection_id]->app_buffer_len;
    
    if (write_pos + payload_len > MAX_SIZE_BUFFER) {
        if (cons[connection_id]->app_buffer_len + payload_len <= MAX_SIZE_BUFFER) {
            memmove(cons[connection_id]->app_buffer, 
                    cons[connection_id]->app_buffer + cons[connection_id]->app_buffer_start, 
                    cons[connection_id]->app_buffer_len);
            cons[connection_id]->app_buffer_start = 0;
            write_pos = cons[connection_id]->app_buffer_len;
        } else {
            return 0;
        }
    }

    memcpy(cons[connection_id]->app_buffer + write_pos, payload, payload_len);
    cons[connection_id]->app_buffer_len += payload_len;
    return 1;
}

void save_future_package(int connection_id, uint16_t seq, char *segment, int total_len) 
{
    int idx = seq % MAX_NUMBER_PKTS;

    if (!cons[connection_id]->out_of_order_window[idx].is_occupied) {
        memcpy(cons[connection_id]->out_of_order_window[idx].pkt.data, segment, total_len);
        cons[connection_id]->out_of_order_window[idx].pkt.len = total_len;
        cons[connection_id]->out_of_order_window[idx].seq_num = seq;
        cons[connection_id]->out_of_order_window[idx].is_occupied = true;
    }
}

void empty_waiting_room(int conn_id) 
{
    for (int i = 0; i < MAX_NUMBER_PKTS; i++) {
        int idx = cons[conn_id]->expected_seq % MAX_NUMBER_PKTS;
        
        if (cons[conn_id]->out_of_order_window[idx].is_occupied && 
            cons[conn_id]->out_of_order_window[idx].seq_num == cons[conn_id]->expected_seq) {
            
            struct poli_tcp_data_hdr *future_hdr = (struct poli_tcp_data_hdr *)cons[conn_id]->out_of_order_window[idx].pkt.data;
            char *future_payload = cons[conn_id]->out_of_order_window[idx].pkt.data + sizeof(struct poli_tcp_data_hdr);
            
            if (save_in_buffer(conn_id, future_payload, future_hdr->len) == 1) {
                cons[conn_id]->out_of_order_window[idx].is_occupied = false;
                cons[conn_id]->expected_seq++;
            } else {
                break;
            }

            //save_in_buffer(conn_id, future_payload, future_hdr->len);
            
            //cons[conn_id]->out_of_order_window[idx].is_occupied = false;
            //cons[conn_id]->expected_seq++;
        } else {
            break;
        }
    }
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        if (cons.size() == 0) {
            usleep(100000); // daca nu avem socketuri evit busy waiting
            continue;
        }
        
        int connection_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &connection_id);
        } while(res == -14);

        if (connection_id == -1 || res <= 0) {
            continue; 
        }

        pthread_mutex_lock(&cons[connection_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)segment;

        if (hdr->type == DATA) {
            uint16_t seq = hdr->seq_num;
            int payload_len = hdr->len;
            char *payload = segment + sizeof(struct poli_tcp_data_hdr); 

            if (seq < cons[connection_id]->expected_seq) { // daca e duplicat, doar se trimite ack ca a fost primit
            } 
            else if (seq > cons[connection_id]->expected_seq) {
                save_future_package(connection_id, seq, segment, res); // daca pachetul a fost trimis inainte de cel care trebuia sa vina, este pus in asteptare
            } 
            else {
                save_in_buffer(connection_id, payload, payload_len);
                cons[connection_id]->expected_seq++; 
                
                // dupa ce vine pachetul cu id-ul asteptat, pun inapoi pachetele care au ajuns mai devreme
                empty_waiting_room(connection_id);
            }

            cons[connection_id]->recv_window_size = MAX_SIZE_BUFFER - cons[connection_id]->app_buffer_len;
            send_ack(connection_id, cons[connection_id]->expected_seq);
        }

        pthread_mutex_unlock(&cons[connection_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    // 1. creez socketul serverului
    static int server_sock = -1;
    
    if (server_sock == -1) {
        server_sock = socket(AF_INET, SOCK_DGRAM, 0);
        struct sockaddr_in server_address = {0};
        server_address.sin_family = AF_INET;
        server_address.sin_port = port; // 8032 (va veni ca htons(8032) din server.cpp)
        server_address.sin_addr.s_addr = ip;

        // bind dupa ce s-a facut socket, specific protocolului TCP
        bind(server_sock, (struct sockaddr*)&server_address, sizeof(server_address));
        printf("Asteptare clienti noi...\n");
    }

    while (true) {
        struct sockaddr_in client_address = {0};
        socklen_t len = sizeof(client_address);
        char buffer[1500] = {0};

        // asteptare syn de la client
        int nr_bytes = recvfrom(server_sock, buffer, sizeof(buffer), 0, (struct sockaddr*)&client_address, &len);
        if (nr_bytes < 0) {
            continue;
        }

        struct poli_tcp_ctrl_hdr *syn_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
        if (syn_hdr->type != SYN) {
            printf("PROBLEMA: Raspunsul nu este un ack la server.\n");
            continue; // am primit gunoi, deci ma opresc
        }
        printf("Serverul a primit syn ACK de la client\n");

        // creare socket si conexiune noua, trebuie dat un port random pentru a nu aglomera unul singur
        // si pentru a trece testul de conexiuni multiple

        struct connection *con = (struct connection *)calloc(1, sizeof(struct connection));
        int conn_id = cons.size(); 
        con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

        struct sockaddr_in new_servaddress = {0};
        new_servaddress.sin_addr.s_addr = INADDR_ANY;
        new_servaddress.sin_family = AF_INET;
        new_servaddress.sin_port = htons(0); // Port 0, adica las sistemul de operare sa aleaga un port liber
        bind(con->sockfd, (struct sockaddr*)&new_servaddress, sizeof(new_servaddress));

        // aflu portul pe care l-am primit mai sus
        socklen_t new_len = sizeof(new_servaddress);
        getsockname(con->sockfd, (struct sockaddr*)&new_servaddress, &new_len);
        uint16_t random_port = new_servaddress.sin_port;
        
        // Setez datele pentru conexiune 
        con->app_buffer_len = 0;
        con->app_buffer_start = 0;
        con->servaddr = client_address;
        con->recv_window_size = MAX_SIZE_BUFFER; 
        con->expected_seq = 0; 
        con->conn_id = conn_id;
        
        // trimitere syn-ack cu noul port
        char send_buffer[1500] = {0};
        struct poli_tcp_data_hdr *syn_ack = (struct poli_tcp_data_hdr *)send_buffer;
        syn_ack->conn_id = conn_id;
        syn_ack->protocol_id = POLI_PROTOCOL_ID;
        syn_ack->type = SYN_ACK;
        
        *(uint16_t*)(send_buffer + sizeof(struct poli_tcp_data_hdr)) = random_port;

        printf("Trimitere syn-ack...\n");
        sendto(server_sock, send_buffer, sizeof(struct poli_tcp_data_hdr) + sizeof(uint16_t), 0, (struct sockaddr*)&client_address, len);

        // asteptare ack final de la client (sender)

        struct timeval tv = {2, 0}; // 2 secunde timeout
        if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
            perror("Eroare la setare timeout\n");
        }

        printf("Asteptare ACK final de la sender...\n");
        int ack_bytes = recvfrom(con->sockfd, buffer, sizeof(buffer), 0, NULL, NULL);
        
        if (ack_bytes < 0) {
            printf("EROARE: Timeout la primirea ACK-ului final. O luam de la capat!\n");
            free(con);  
            continue;
        }
        
        // se face cast la header pentru a putea verifica daca este ack
        struct poli_tcp_ctrl_hdr *ack_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
        
        if (ack_hdr->type == ACK) {
            printf("Handshake finalizat, sa inceapa transmiterea de date...\n");

            struct timeval tv_zero = {0, 0};
            setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv_zero, sizeof(tv_zero));
        } else {
            printf("EROARE: Nu s-a primit ack\n");
            free(con);
            continue; // daca este pachet invalid s-a stricat tot handshake-ul
        }

        /* Since we can have multiple connection, we want to know if data is available
        on the socket used by a given connection. We use POLL for this */

        pthread_mutex_lock(&poll_lock);

        data_fds[fdmax].fd = con->sockfd;    
        data_fds[fdmax].events = POLLIN;    
        
        /* This creates a timer and sets it to trigger every 1 sec. We use this
        to know if a timeout has happend on a connection */
        timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
        timer_fds[fdmax].events = POLLIN;    
        struct itimerspec spec;     
        spec.it_value.tv_sec = 1;    
        spec.it_value.tv_nsec = 0;    
        spec.it_interval.tv_sec = 1;    
        spec.it_interval.tv_nsec = 0;    
        timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
        fdmax++;    

        pthread_mutex_unlock(&poll_lock);

        pthread_mutex_init(&con->con_lock, NULL);
        cons.insert({conn_id, con});

        DEBUG_PRINT("Connection established!");

        return conn_id;
    }
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
