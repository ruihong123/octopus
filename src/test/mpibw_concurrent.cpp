#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif
#define TEST_NRFS_IO
//#define TEST_RAW_IO
#include "mpi.h"
#include "nrfs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <thread>
#include <condition_variable>
#include <mutex>

#define BUFFER_SIZE 0x1000000 //4MB data
#define THREAD_NUM 16
int myid, file_seq;
int numprocs;
nrfs fs;

char buf[THREAD_NUM][BUFFER_SIZE];
int mask = 0;
int thread_num;
bool test_start = false;
int thread_ready_num = 0;
int thread_finish_num = 0;
std::mutex startmtx;
std::mutex finishmtx;
std::condition_variable cv;
int collect_time(int cost)
{
	int i;
	char message[8];
	MPI_Status status;
	int *p = (int*)message;
	int max = cost;
	for(i = 1; i < numprocs; i++)
	{
		MPI_Recv(message, 8, MPI_CHAR, i, 99, MPI_COMM_WORLD, &status);
		if(*p > max)
			max = *p;
	}
	return max;
}
void nrfsWrite_test(nrfs fs, nrfsFile _file, const void* buffer, uint64_t size, uint64_t offset){
    std::unique_lock<std::mutex> lck_start(startmtx);

    thread_ready_num++;
    printf("thread ready %d\n", thread_ready_num);
    while (!test_start){
        cv.wait(lck_start);

    }

    lck_start.unlock();
    printf("thread start to write.\n");
    nrfsWrite(fs, _file, buffer, size, offset);
    std::unique_lock<std::mutex> lck_end(startmtx);
    thread_finish_num++;
    if (thread_finish_num >= thread_num) {
        cv.notify_all();
    }
    lck_end.unlock();

}

void write_test(int size, int op_time)
{
	char path[255];
	int i;
	double start, end, rate, num;
	int time_cost;
	char message[8];
	int *p = (int*)message;

	/* file open */
	sprintf(path, "/file_%d", file_seq);
	nrfsOpenFile(fs, path, O_CREAT);
	printf("create file: %s\n", path);
	memset(buf, 'a', BUFFER_SIZE);

	MPI_Barrier ( MPI_COMM_WORLD );
    std::thread* t[op_time];
	start = MPI_Wtime();
	for(i = 0; i < op_time; i++)
	{
#ifdef TEST_RAW_IO
		nrfsRawWrite(fs, path, buf, size, 0);
#endif
#ifdef TEST_NRFS_IO
        t[i] = new std::thread(nrfsWrite_test, fs, path, buf[i], size, 0);
       t[i]->detach();
       printf("thread has been issued.\n");
//		nrfsWrite(fs, path, buf[i], size, 0);
#endif
	}
    std::unique_lock<mutex> l_s(startmtx);
    while (thread_ready_num!= op_time){
        cv.wait(l_s);
    }
    test_start = true;
    cv.notify_all();
    l_s.unlock();
    std::unique_lock<mutex> l_e(finishmtx);

    while (thread_finish_num < thread_num) {
        cv.wait(l_e);
    }
    printf("thread has finished.\n");
    l_e.unlock();
//    for(i = 0; i < op_time; i++){
//        t[i]->join();
//    }
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );

	*p = (int)((end - start) * 1000000);

	if(myid != 0)
	{
		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
	}
	else
	{
		time_cost = collect_time(*p);
		num = (double)(size * op_time * numprocs) / time_cost;
		rate = 1000000 * num / 1024 / 1024;
		printf("Write Bandwidth = %f MB/s TimeCost = %d\n", rate, (int)time_cost);
	}
	nrfsCloseFile(fs, path);

	file_seq += 1;
	if(file_seq == numprocs)
		file_seq = 0;
}

void read_test(int size, int op_time)
{
	char path[255];
	int i;
	double start, end, rate, num;
	int time_cost;
	char message[8];
	int *p = (int*)message;

	memset(buf, '\0', BUFFER_SIZE);
	memset(path, '\0', 255);
	sprintf(path, "/file_%d", file_seq);

	MPI_Barrier ( MPI_COMM_WORLD );
	
	start = MPI_Wtime();
	std::thread* t[10];
	for(i = 0; i < op_time; i++)
	{
#ifdef TEST_RAW_IO
		nrfsRawRead(fs, path, buf, size, 0);
#endif
#ifdef TEST_NRFS_IO
        t[i] = new std::thread(nrfsRead, fs, path, buf[i], size, 0);

//		nrfsRead(fs, path, buf[i], size, 0);
#endif
	}
    for(i = 0; i < op_time; i++){
        t[i]->join();
    }
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );

	*p = (int)((end - start) * 1000000);

	if(myid != 0)
	{
		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
	}
	else
	{
		time_cost = collect_time(*p);
		num = (double)(size * op_time * numprocs) / time_cost;
		rate = 1000000 * num / 1024 / 1024;
		printf("Read Bandwidth = %f MB/s TimeCost = %d\n", rate, (int)time_cost);
	}
	MPI_Barrier ( MPI_COMM_WORLD );

	file_seq += 1;
	if(file_seq == myid)
		file_seq = 0;
}


int main(int argc, char **argv)
{

	char path[255];
	if(argc < 3)
	{
		fprintf(stderr, "Usage: ./mpibw block_size\n");
		return -1;
	}
	int block_size = atoi(argv[1]);
	int op_time = atoi(argv[2]);
    thread_num = op_time;
	MPI_Init( &argc, &argv);
	MPI_Comm_rank( MPI_COMM_WORLD, &myid );
	MPI_Comm_size( MPI_COMM_WORLD, &numprocs );
	file_seq = myid;
	MPI_Barrier ( MPI_COMM_WORLD );

	/* nrfs connection */
	fs = nrfsConnect("default", 0, 0);

	MPI_Barrier ( MPI_COMM_WORLD );

	write_test(1024 * block_size, op_time);
	read_test(1024 * block_size, op_time);

	MPI_Barrier ( MPI_COMM_WORLD );
	sprintf(path, "/file_%d", myid);
	nrfsDelete(fs, path);
	nrfsDisconnect(fs);

	MPI_Finalize();
}
