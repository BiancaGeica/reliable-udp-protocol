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
#define ACK 4

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

pthread_mutex_t poll_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t id_lock = PTHREAD_MUTEX_INITIALIZER;
static int next_conn_id = 0;

static std::map<uint16_t, bool> seen_ports;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    while (cons[conn_id]->app_buffer_len == 0) {
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
        usleep(1000); 
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }

    if (len < cons[conn_id]->app_buffer_len) {
        size = len;
    } else {
        size = cons[conn_id]->app_buffer_len;
    }

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
    
    int available = MAX_SIZE_BUFFER - cons[connection_id]->app_buffer_len;
    if (available > 65535) {
        available = 65535;
    }
    ack.recv_window = available;

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
            return 0; // Aruncam daca e plin
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
        } else {
            break;
        }
    }
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;

    while (1) {
        if (cons.size() == 0) {
            usleep(100000); 
            continue;
        }
        
        int connection_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &connection_id);
        } while(res == -14);

        if (connection_id == -1 || res <= 0) continue; 
        if (cons.count(connection_id) == 0 || cons[connection_id] == NULL) continue; 

        pthread_mutex_lock(&cons[connection_id]->con_lock);

        struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)segment;

        if (hdr->type == DATA) {
            uint16_t seq = hdr->seq_num;
            int payload_len = hdr->len;
            char *payload = segment + sizeof(struct poli_tcp_data_hdr); 

            int16_t diff = (int16_t)(seq - cons[connection_id]->expected_seq);

            if (diff < 0) { 
            } 
            else if (diff > 0) {
                save_future_package(connection_id, seq, segment, res); 
            } 
            else {
                // FIXUL FATAL: Doar daca am bagat in buffer incrementam
                if (save_in_buffer(connection_id, payload, payload_len) == 1) {
                    cons[connection_id]->expected_seq++; 
                    empty_waiting_room(connection_id);
                }
            }

            cons[connection_id]->recv_window_size = MAX_SIZE_BUFFER - cons[connection_id]->app_buffer_len;
            send_ack(connection_id, cons[connection_id]->expected_seq);
        }

        pthread_mutex_unlock(&cons[connection_id]->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    static int server_sock = -1;
    
    if (server_sock == -1) {
        server_sock = socket(AF_INET, SOCK_DGRAM, 0);
        struct sockaddr_in server_address = {0};
        server_address.sin_family = AF_INET;
        server_address.sin_port = port; 
        server_address.sin_addr.s_addr = ip;

        bind(server_sock, (struct sockaddr*)&server_address, sizeof(server_address));
    }

    while (true) {
        struct sockaddr_in client_address = {0};
        socklen_t len = sizeof(client_address);
        char buffer[1500] = {0};

        int nr_bytes = recvfrom(server_sock, buffer, sizeof(buffer), 0, (struct sockaddr*)&client_address, &len);
        if (nr_bytes < 0) {
            continue;
        }

        struct poli_tcp_ctrl_hdr *syn_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
        if (syn_hdr->type != SYN) {
            continue; 
        }

        uint16_t c_port = client_address.sin_port;
        if (seen_ports.find(c_port) != seen_ports.end()) {
            continue; 
        }
        seen_ports[c_port] = true;

        struct connection *con = (struct connection *)calloc(1, sizeof(struct connection));
        
        pthread_mutex_lock(&id_lock);
        int conn_id = next_conn_id++;
        pthread_mutex_unlock(&id_lock);
        
        con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

        struct sockaddr_in new_servaddress = {0};
        new_servaddress.sin_addr.s_addr = INADDR_ANY;
        new_servaddress.sin_family = AF_INET;
        new_servaddress.sin_port = htons(0); 
        bind(con->sockfd, (struct sockaddr*)&new_servaddress, sizeof(new_servaddress));

        socklen_t new_len = sizeof(new_servaddress);
        getsockname(con->sockfd, (struct sockaddr*)&new_servaddress, &new_len);
        uint16_t random_port = new_servaddress.sin_port;
        
        con->app_buffer_len = 0;
        con->app_buffer_start = 0;
        con->servaddr = client_address;
        con->recv_window_size = MAX_SIZE_BUFFER; 
        con->expected_seq = 0; 
        con->conn_id = conn_id;
        
        char send_buffer[1500] = {0};
        struct poli_tcp_data_hdr *syn_ack = (struct poli_tcp_data_hdr *)send_buffer;
        syn_ack->conn_id = conn_id;
        syn_ack->protocol_id = POLI_PROTOCOL_ID;
        syn_ack->type = SYN_ACK;
        *(uint16_t*)(send_buffer + sizeof(struct poli_tcp_data_hdr)) = random_port;

        struct timeval tv = {1, 0}; 
        setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        while (true) {
            sendto(server_sock, send_buffer, sizeof(struct poli_tcp_data_hdr) + sizeof(uint16_t), 0, (struct sockaddr*)&client_address, len);
            
            int ack_bytes = recvfrom(con->sockfd, buffer, sizeof(buffer), 0, NULL, NULL);
            if (ack_bytes >= 0) {
                struct poli_tcp_ctrl_hdr *ack_hdr = (struct poli_tcp_ctrl_hdr *)buffer;
                if (ack_hdr->type == ACK || ack_hdr->type == DATA) {
                    break;
                }
            }
        }

        struct timeval tv_zero = {0, 0};
        setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv_zero, sizeof(tv_zero));

        pthread_mutex_lock(&poll_lock);

        data_fds[fdmax].fd = con->sockfd;    
        data_fds[fdmax].events = POLLIN;    
        
        timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
        timer_fds[fdmax].events = POLLIN;    
        struct itimerspec spec;     
        spec.it_value.tv_sec = 1;    
        spec.it_value.tv_nsec = 0;    
        spec.it_interval.tv_sec = 1;    
        spec.it_interval.tv_nsec = 0;    
        timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
        fdmax++;    

        cons.insert({conn_id, con});

        pthread_mutex_unlock(&poll_lock);

        pthread_mutex_init(&con->con_lock, NULL);

        return conn_id;
    }
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;
    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}