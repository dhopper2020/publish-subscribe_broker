#include "BXP/bxp.h"
#include "ADTs/hashmap.h"
#include "ADTs/queue.h"
#include "sort.h"
#include <valgrind/valgrind.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#define UNUSED __attribute__((unused))
#define MAXWORD 200
#define MAXSUB 256
#define MAXCHAN 256

//setting up my thread variable for responses and publishing
pthread_t reqthread;
pthread_t pubthread;
//setting up conditional variable and lock variables
pthread_mutex_t publishLock;
pthread_cond_t publishCond;

//setting up channel and subscriber ids 
//chid will be direct indexs into my channel array
long chid = -1;
long svid = -1;

//need to define above subscriber because subsriber will need an array of channels that
//it is subscribed to
typedef struct channel Channel;

//creating subscriber struct
typedef struct subscriber
{
	char name[MAXWORD];
	char clid[MAXWORD];
	char host[MAXWORD];
	char service[MAXWORD];
	unsigned short port;
	long svid;
	bool unSub;
}Sub;

//creating channel struct
//going to need a mutex lock for each channel I make, so I a mutex lock variable 
//in struct
typedef struct channel
{
	char name[MAXWORD];
	int numSub;
	Sub subArray[MAXSUB];
	long chid;
	bool isDeleted;
	pthread_mutex_t channelLock;
} Channel;

//Array that holds all of the channls
Channel cArray[MAXCHAN];
//setting up my queue which I will define later and will use for my publish thread
const Queue *publishQ = NULL;

//signal to exit when ctrl c is entered in 
void onint(UNUSED int sig) 
{
	printf("\nRecieved signal, exiting\n");
	pthread_cancel(reqthread);
	exit(EXIT_SUCCESS);
}

//function that creates a channel and returns its chid
long createChannel(char *name)
{
	//search to see if there already exists a channel return it's chid
	for (int i = 0; i <= chid; i++)
	{
		if (!strcmp(cArray[i].name, name))
		{
			return cArray[i].chid;
		}
	}
	chid++;
	//intialize all the values that channel holds
	pthread_mutex_init(&(cArray[chid].channelLock), NULL);
	strcpy(cArray[chid].name, name);
	cArray[chid].chid = chid;
	cArray[chid].numSub = 0;
	cArray[chid].isDeleted = false;
	
	return cArray[chid].chid;
}

//function that lists all the channels in cArray
void listChannels(char **list)
{
	//first bubblesort all the channels in alphabetical order
	int k = chid;
	for (int i = 0; i < k; i++)
	{
		for (int j = 0; j < k - i; j++)
		{
			if (cArray[j+1].name[0] < cArray[j].name[0])
			{
				if (!cArray[j].isDeleted)
				{
					char tmp[100];
					strcpy(tmp, cArray[j].name);
					strcpy(cArray[j].name, cArray[j+1].name);
					strcpy(cArray[j+1].name, tmp);
				}
			}
		}
	}
	//then add it to a temporary char so that you can return it with commas
	char word[MAXWORD];
	word[0] = '\0';
	for (int i = 0; i <= chid; i++)
	{
		if (!cArray[i].isDeleted)
		{
			strcat(word, cArray[i].name);
			if (i != chid)
			{
				strcat(word, ", ");
			}
		}
	}
	//if no channels have been created, then chid will be -1 and we will return a string showing such
	if (chid == -1)
	{
		strcpy(word, "No channels defined");
	}
	strcat(word, "\n");

	strcpy(*list, word);
	
}

//function that allows a subscriber to subscribe to a channel
long Subscribe(char *name, char *clid, char *host, char *service, unsigned short port)
{
	int i;
	long check = 0;
	for (i = 0; i <= chid; i++)
	{
		if (!strcmp(cArray[i].name, name))
		{
			if (cArray[i].isDeleted)
			{
				return -3;
			}
			check = 1;
			pthread_mutex_lock(&cArray[i].channelLock);
			svid++;
			//this is using the sub array in the channel array to set all a subscribers values
			cArray[i].subArray[cArray[i].numSub].svid = svid;
			cArray[i].subArray[cArray[i].numSub].port = port;
			strcpy(cArray[i].subArray[cArray[i].numSub].clid, clid);
			strcpy(cArray[i].subArray[cArray[i].numSub].name, name);
			strcpy(cArray[i].subArray[cArray[i].numSub].host, host);
			strcpy(cArray[i].subArray[cArray[i].numSub].service, service);
			cArray[i].subArray[cArray[i].numSub].unSub = false;

			cArray[i].numSub++;
			pthread_mutex_unlock(&cArray[i].channelLock);
			break;
		}
	}
	//check if the channel subscribed to exists or not
	if (!check)
		return -2;

	return svid;
}

//function that allows a subscriber to unsubscriber by changing the unSub bool variable to false
long Unsubscribe(long id)
{
	int i, j;
	long check = 0;
	//go through all the channel arrays and sub arrays and find the ones that have a matching svid
	for (i = 0; i <= chid; i++)
	{
		for (j = 0; j < cArray[i].numSub; j++)
		{
			if (id == cArray[i].subArray[j].svid)
			{
				if (cArray[i].isDeleted)
				{
					return -3;
				}
				check = 1;
				pthread_mutex_lock(&cArray[i].channelLock);
				cArray[i].subArray[j].unSub = true;
				pthread_mutex_unlock(&cArray[i].channelLock);
			}
		}
	}
	//check if the svid was found or not
	if (!check)
		return -2;

	return svid;
}

//Finds the correct channel and then stores all the names of the subscribers into a varible and 
//then returns it
void listSubscribers(char *name, char **list)
{
	//varibles to check if there is a channel with the given parameter and to check to see if there
	//is a channel named, if there are subsribers subscribed to it
	int checkChan = 0;
	int checkSub = 0;

	//go through the whole channel array, find the correct channel, then out put all of the 
	//subscribers that that channel holds if they are still subscribed
	for (int i = 0; i <= chid; i++)
	{
		if (!strcmp(cArray[i].name, name))
		{
			if (cArray[i].isDeleted)
			{
				strcat(*list, "Channel is deleted\n");
				return;
			}
			checkChan = 1;
			for (int j = 0; j < cArray[i].numSub; j++)
			{
				checkSub = 1;
				pthread_mutex_lock(&cArray[i].channelLock);
				if (!cArray[i].subArray[j].unSub)
				{
					strcat(*list, cArray[i].subArray[j].clid);
					if (j != cArray[i].numSub - 1)
					{
						strcat(*list, "\n");
					}
				}
				pthread_mutex_unlock(&cArray[i].channelLock);
			}
		}
	}
	if (!checkChan)
	{
		strcat(*list, "No channel named ");
		strcat(*list, name);
		strcat(*list, "\n");
	}
	if (!checkSub)
	{
		strcat(*list, "No subscriptions to ");
		strcat(*list, name);
		strcat(*list, "\n");
	}

}

//helper function to parse words
int extractWords(char *buf, char *sep, char *words[]) {
    int i;
    char *p;

    for (p = strtok(buf, sep), i = 0; p != NULL; p = strtok(NULL, sep), i++)
        words[i] = p;
    words[i] = NULL;
    return i;
}

//helper function that puts name and message into a queue which will then be dequeued by a created 
//thread named publishThread

char *Publish(char *name, char *message)
{
	//will be enqueueing strings that we will extract

	//this is to check if the given name is in the channel array
	//if it is, immediately return
	int check = 0;
	for (int i = 0; i <= chid; i++)
	{
		if (!strcmp(cArray[i].name, name) && (cArray[i].isDeleted == false))
			check = 1;
	}
	if (!check)
	{
		char *word1 = (char *)malloc(sizeof(char) * MAXWORD);
		word1[0] = '\0';
		strcat(word1, "No channel named ");
		strcat(word1, name);
		strcat(word1, "\n");
		return word1;
	}

	//put the word on the heap so that we can use it in our queue thread function
	char *word = (char *)malloc(sizeof(char) * MAXWORD);
	word[0] = '\0';
	strcat(word, name);
	strcat(word, "*");
	strcat(word, message);

	//enqueing a string that contains the channel name and message that we will use the extract word
	//fucntion to get
	pthread_mutex_lock(&publishLock);
	check = publishQ->enqueue(publishQ, (void *)word);
	pthread_cond_signal(&publishCond);
	pthread_mutex_unlock(&publishLock);
	return message;
}

//thread that dequeues from queue then sends a message to all the subscribed subscribers

void *publishThread()
{
	BXPConnection bxpc;
	unsigned reqlen, resplen;
	char req[MAXWORD];
	char resp[MAXWORD];
	while (1)
	{
		//wait for the signal, so you know the queue is no longer empty so you can dequeue and
		//work with data
		pthread_mutex_lock(&publishLock);
		while (publishQ->isEmpty(publishQ))
		{
			pthread_cond_wait(&publishCond, &publishLock);
		}
		
		char *word;
		char *tmp[500];

		publishQ->dequeue(publishQ, (void **)&word);
		extractWords(word, "*", tmp);
		strcpy(req, tmp[1]);
		reqlen = (unsigned)strlen(tmp[1]);

		//go through the channel array until you get the right channel, then go through the 
		//all the subscribers subscribed to that channel and send them a message if they are 
		//still subscribed
		for (int i = 0; i <= chid; i++)
		{
			if ((!strcmp(cArray[i].name, tmp[0])) && (cArray[i].isDeleted == false))
			{
				for (int j = 0; j < cArray[i].numSub; j++)
				{
					if (!cArray[i].subArray[j].unSub)
					{
						bxpc = bxp_connect(cArray[i].subArray[j].host, cArray[i].subArray[j].port, 
							cArray[i].subArray[j].service, 1, 1);
						reqlen = (unsigned)(strlen(req) + 1);

						bxp_call(bxpc, (void *)req, reqlen, (void *)resp, 
							(unsigned)sizeof(resp), &resplen);
						bxp_disconnect(bxpc);
					}
				}
			}
		}
		free(word);
		pthread_mutex_unlock(&publishLock);
	}
}

long destroyChannel(long chanID)
{
	bool check = false;
	for (int i = 0; i <= chid; i++)
	{
		if (cArray[i].chid == cArray[chanID].chid)
			check = true;
	}
	if (!check)
	{
		return -2;
	}

	cArray[chanID].isDeleted = true;

	pthread_mutex_lock(&publishLock);
	long size = publishQ->size(publishQ);
	for (int i = 0; i < size; i++)
	{
		char *word;
		char *tmp[500];
		publishQ->dequeue(publishQ, (void **)&word);
		extractWords(word, "*", tmp);
		if (strcmp(cArray[chanID].name, tmp[0]))
			publishQ->enqueue(publishQ, (void *)word);
	}
	pthread_mutex_unlock(&publishLock);
	return chanID;
}

//this is a function thread that takes requests from clients and preforms a service for them 
//depending on what they enter and if they enter it correctly
void *requests()
{
	assert(bxp_init(19998, 1));
	BXPEndpoint ep;
    BXPService svc;
    char query[10000], response[10001];
    unsigned qlen, rlen;

	assert((svc = bxp_offer("PSB")));
    while ((qlen = bxp_query(svc, &ep, query, 10000)) > 0) 
    {
    	char queryArray[1000];
    	char *tmp[100];
    	strcpy(queryArray, query);
    	char *p = strrchr(queryArray, '\n');
    	if (p != NULL)
    	{
    		*p = '\0';
    	}
    	int count = extractWords(queryArray, "|", tmp);
    	char test[100];
    	strcpy(test, tmp[0]);

		if ( (!strcmp("CreateChannel", test)) && (count == 2) )
		{
			long chid = createChannel(tmp[1]);
			sprintf(response, "1%lu", chid);
		}
		else if ( (!strcmp("DestroyChannel", test)) && (count == 2) )
		{
			long check = destroyChannel(atol(tmp[1]));
			if (check == -2)
			{
				char wordTmp[MAXWORD];
				wordTmp[0] = '\0';
				strcat(wordTmp, "No channel matching ");
				strcat(wordTmp, tmp[1]);
				strcat(wordTmp, "\n");

				sprintf(response, "0%s", wordTmp);
			}
			else
				sprintf(response, "1%ld", check);
		}
		else if ( (!strcmp("ListChannels", test)) && (count == 1) )
		{
			char *list = (char *)malloc(sizeof(char) * MAXWORD);
			list[0] = '\0';
			listChannels(&list);
			//checking to see if the string returned with no channels
			if (!strcmp(list, "No channels defined\n"))
				sprintf(response, "0%s", query);
			else
				sprintf(response, "1%s", list);
			free(list);
		}
		else if ( (!strcmp("ListSubscribers", test)) && (count == 2) )
		{
			//creating a string that I can give to the client for error
			//tmpChan is for when it can't find a channel
			//tmpSub is for when there are no subscribers to a channel
			char *list = (char *)malloc(sizeof(char) * MAXWORD);
			list[0] = '\0';
			listSubscribers(tmp[1], &list);
			char tmpChan[MAXWORD];
			char tmpSub[MAXWORD];
			tmpChan[0] = '\0';
			tmpSub[0] = '\0';
			strcat(tmpChan, "No channel defined ");
			strcat(tmpChan, tmp[1]);
			strcat(tmpChan, "\n");
			strcat(tmpSub, "No subscriptions to ");
			strcat(tmpSub, tmp[1]);
			strcat(tmpSub, "\n");
			if (!strcmp(list, tmpChan))
				sprintf(response, "0%s", tmpChan);
			else if (!strcmp(list, tmpSub))
				sprintf(response, "0%s", tmpSub);
			else
				sprintf(response, "1%s", list);
			free(list);
		}
		
		else if ( (!strcmp("Publish", test)) && (count == 3) )
		{
			char *msg = Publish(tmp[1], tmp[2]);

			char wordTmp[MAXWORD];
			wordTmp[0] = '\0';
			strcat(wordTmp, "No channel named ");
			strcat(wordTmp, tmp[1]);
			strcat(wordTmp, "\n");

			if (!strcmp(wordTmp, msg))
			{
				sprintf(response, "0%s", wordTmp);
				free(msg);
			}
			else
				sprintf(response, "1%s", msg);
			free(msg);
		}
		
		else if ( (!strcmp("Subscribe", test)) && (count == 6) )
		{
			long tmp_svid = Subscribe(tmp[1], tmp[2], tmp[3], tmp[4], (unsigned short)atoi(tmp[5]));
			if (tmp_svid == -2)
			{
				//creating a string that I can give to the client for error
				char wordTmp[MAXWORD];
				wordTmp[0] = '\0';
				strcat(wordTmp, "No channel named ");
				strcat(wordTmp, tmp[1]);
				strcat(wordTmp, "\n");

				sprintf(response, "0%s", wordTmp);
			}
			else if (tmp_svid == -3)
			{
				//creating a string that I can give to the client for error
				char wordTmp[MAXWORD];
				wordTmp[0] = '\0';
				strcat(wordTmp, "Channel deleted");
				strcat(wordTmp, "\n");

				sprintf(response, "0%s", wordTmp);
			}
			else
				sprintf(response, "1%lu", svid);
		}
		
		else if ( (!strcmp("Unsubscribe", test)) && (count == 2) )
		{
			long tmp_svid = Unsubscribe(atoi(tmp[1]));
			if (tmp_svid == -2)
			{
				//creating a string that I can give to the client for error
				char wordTmp[MAXWORD];
				wordTmp[0] = '\0';
				strcat(wordTmp, "No subscription matching ");
				strcat(wordTmp, tmp[1]);
				strcat(wordTmp, "\n");

				sprintf(response, "0%s", wordTmp);
			}
			else
				sprintf(response, "1%lu", tmp_svid);
		}
		else
		{
			sprintf(response, "0%s", query);
		}

        rlen = strlen(response) + 1;
		assert(bxp_response(svc, &ep, response, rlen));
    }
    return NULL;
}

int main(int argc, char **argv)
{
	//for checking memory leaks
	VALGRIND_MONITOR_COMMAND("leak_check summary");

	//creating queue
	publishQ = Queue_create(NULL);


	//getting file from cmdline
	char filename[100];
	if (getopt(argc, argv, "f:") == 'f') 
	{
			strcpy(filename, optarg);
	}
	else
	{
		fprintf(stderr, "Incorrect usage.  ./psbv?.c -f [file name here]\n");
	}

	//opening file then putting it out on std output
	FILE *fp;
	char *line;
	ssize_t read;
	size_t len = 0;

	fp = fopen(filename, "r");
	if (fp == NULL)
		exit(EXIT_FAILURE);
	
	//getting lines from the file to then create channels for
	while ((read = getline(&line, &len, fp)) != -1)
	{
		printf("Creating publish/subscribe channel: %s\n", line);
		line[strcspn(line, "\n")] = 0;
		createChannel(line);
	}

	fclose(fp);
	if (line)
		free(line);

	//listening for signal
    signal(SIGINT, onint);

    //create thread for listening to requests
    pthread_create(&reqthread, NULL, requests, NULL);
    //create thread that will publish messages
    pthread_create(&pubthread, NULL, publishThread, NULL);
  
  	//waits until the requests thread is done, which should never happen unless the 
  	//thread is canceled by the signal that we set up
    pthread_join(reqthread, NULL);
    pthread_join(pubthread, NULL);
    return EXIT_SUCCESS;
}

