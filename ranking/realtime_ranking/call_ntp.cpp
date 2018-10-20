#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include "ac_type.h"
#include "call_data.h"

extern ACServer *g_server;

//��ʼ�������
static void* call_ntp_data(oop_source_t *op, int fd, oop_event_t e, void *args){
	int ret;
	struct sockaddr_in addr;
	struct timeval tv_client, tv_server;
	socklen_t addr_len;
	addr_len = sizeof(addr);
	ret = recvfrom(fd, &tv_client, sizeof(tv_client), 0, (struct sockaddr*)&addr, &addr_len);
	if(ret == sizeof(tv_client)){
		gettimeofday(&tv_server, NULL);
		sendto(fd, &tv_server, sizeof(tv_server), 0, (struct sockaddr*)&addr, addr_len);
	}
	return OOP_CONTINUE;
}

//����udp����
static int udp_listen(int port){
	struct sockaddr_in addr = {0};
	int sock, ret;

	//��ʼ��socket(UDPЭ��)
	sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(sock < 0){
		return -1;
	}

	//�󶨶˿�
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;
	ret = bind(sock, (struct sockaddr*)&addr, sizeof(addr));
	if(ret < 0){
		close(sock);
		return -1;
	}
	ret = 0;
	setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char*)&ret, sizeof(ret));

	return sock;
}

//����ntp����
int ntp_start(){
	int port = conf_get_int(g_server->conf_current, "server_port", CONF_SERVER_PORT);
	int sock = udp_listen(port);
	if(sock < 0){
		return -1;
	}
	oop_add_fd(g_server->oop, sock, OOP_READ, call_ntp_data, NULL);
	return 0;
}
