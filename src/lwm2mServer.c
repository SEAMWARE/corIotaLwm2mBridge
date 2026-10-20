//
// FILE            lwm2mServer.c
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// The LwM2M server - see header.
//
// Threads: one thread reads the socket, answers the clients, retransmits what is not acknowledged and lets
// registrations lapse; publish() and the Executes run on broker threads and only send. One mutex guards
// the clients, the DataChannels and the requests in flight; the broker is called (sampleIn,
// serviceUpdateIn) with it released - what the thread has for the broker is collected first (Outbox).
//
#include <arpa/inet.h>                                // inet_ntop
#include <errno.h>                                    // errno
#include <math.h>                                     // isfinite
#include <netinet/in.h>                               // sockaddr_in6
#include <poll.h>                                     // poll
#include <pthread.h>                                  // pthread_*
#include <stdbool.h>                                  // bool
#include <stdint.h>                                   // uint32_t
#include <stdio.h>                                    // snprintf
#include <stdlib.h>                                   // malloc, calloc, free, strtod, strtoll
#include <string.h>                                   // strcmp, strncmp, strchr, strdup, strlen, memcpy, memcmp, strtok_r, strerror
#include <sys/socket.h>                               // socket, bind, sendto, recvfrom
#include <time.h>                                     // clock_gettime
#include <unistd.h>                                   // close

#include "corLog/corLog.h"                            // COR_E, COR_W, COR_I, COR_V
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corAlloc/corAllocBufferInit.h"              // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"             // corAllocBufferReset
#include "corJson/CorJson.h"                          // CorJson
#include "corJson/corJsonCreate.h"                    // corJsonCreate
#include "corJson/corJsonParse.h"                     // corJsonParse
#include "corJson/corJsonRender.h"                    // corJsonFastRender
#include "corJson/corJsonRenderSize.h"                // corJsonFastRenderSize
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeString, corTreeInteger, corTreeFloat, corTreeChildAdd
#include "corTree/corTreeLookup.h"                    // corTreeLookup

#include "corBridge/BridgeBroker.h"                   // BridgeBroker, BRIDGE_*

#include "coap.h"                                     // CoapMessage, coap*
#include "lwm2mServer.h"                              // Own interface



// -----------------------------------------------------------------------------
//
// Observe - a resource a client is asked to report, by the token its notifications carry
//
typedef struct Observe
{
  char             path[32];
  unsigned char    token[8];
  struct Observe*  next;
} Observe;



// -----------------------------------------------------------------------------
//
// Client - a registered LwM2M client
//
typedef struct Client
{
  char                     ep[128];                   // its endpoint name
  char                     regId[16];                 // the registration's location: /rd/<regId>
  struct sockaddr_storage  addr;
  socklen_t                addrLen;
  long                     lifetime;                  // seconds
  double                   lastSeen;                  // its registration or update
  Observe*                 observeList;
  struct Client*           next;
} Client;



// -----------------------------------------------------------------------------
//
// Endpoint - a DataChannel's: a client's name, or a prefix of names (prefix true), and what it observes
//
#define PATHS_MAX  32

typedef struct Endpoint
{
  char*             endpoint;
  char*             name;                             // the endpoint without its '*'
  bool              prefix;
  char              pathV[PATHS_MAX][32];
  int               paths;
  struct Endpoint*  next;
} Endpoint;



// -----------------------------------------------------------------------------
//
// Request - a request to a client in flight: retransmitted until acknowledged, then waited on for its response
//
typedef enum RequestKind
{
  RequestObserve,
  RequestRead,
  RequestWrite,
  RequestExecute
} RequestKind;

typedef struct Request
{
  RequestKind              kind;
  int                      mid;
  unsigned char            token[8];
  char                     ep[128];
  char                     path[32];
  char*                    executionId;               // an Execute's
  unsigned char            dgram[1152];
  int                      len;
  struct sockaddr_storage  addr;
  socklen_t                addrLen;
  double                   nextSend;
  int                      sends;
  bool                     acked;
  double                   deadline;                  // a response no later than this
  struct Request*          next;
} Request;



// -----------------------------------------------------------------------------
//
// Outbox - what the thread has for the broker, handed over with the mutex released
//
typedef struct Outbox
{
  char*           endpoint;                           // a record: on this DataChannel
  char*           json;
  char*           executionId;                        // or an Execute ended
  bool            ok;
  char            why[256];
  struct Outbox*  next;
} Outbox;



#define ACK_TIMEOUT        2.0                        // RFC 7252: the first retransmission's wait
#define MAX_SENDS          5                          // the first and 4 retransmissions
#define RESPONSE_WAIT     60.0                        // a separate response no later than this after the ack
#define REQUESTS_MAX    1024

static const BridgeBroker*  brokerP      = NULL;
static Lwm2mExecuteEnded    executeEnded = NULL;
static int                  sock         = -1;
static pthread_t            thread;
static volatile bool        stopping     = false;
static pthread_mutex_t      mutex        = PTHREAD_MUTEX_INITIALIZER;
static Client*              clientList   = NULL;
static Endpoint*            endpointList = NULL;
static Request*             requestList  = NULL;
static int                  requests     = 0;
static Outbox*              outbox       = NULL;
static Outbox*              outboxTail   = NULL;
static unsigned int         nextMid      = 0;
static uint32_t             nextToken    = 0;
static unsigned int         nextRegId    = 0;



// -----------------------------------------------------------------------------
//
// now - seconds, monotonic
//
static double now(void)
{
  struct timespec ts;

  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec / 1e9;
}



// -----------------------------------------------------------------------------
//
// addrText - a client's address, for the log
//
static const char* addrText(const struct sockaddr_storage* aP, char* buf, int size)
{
  char host[64] = "?";
  int  port     = 0;

  if (aP->ss_family == AF_INET6)
  {
    const struct sockaddr_in6* a6P = (const struct sockaddr_in6*) aP;

    inet_ntop(AF_INET6, &a6P->sin6_addr, host, sizeof(host));
    port = ntohs(a6P->sin6_port);
  }
  else if (aP->ss_family == AF_INET)
  {
    const struct sockaddr_in* a4P = (const struct sockaddr_in*) aP;

    inet_ntop(AF_INET, &a4P->sin_addr, host, sizeof(host));
    port = ntohs(a4P->sin_port);
  }

  snprintf(buf, size, "%s:%d", host, port);
  return buf;
}



// -----------------------------------------------------------------------------
//
// outboxRecord / outboxEnded - under the mutex; outboxDeliver - with it released
//
static void outboxAdd(Outbox* oP)
{
  if (outboxTail == NULL)
    outbox = oP;
  else
    outboxTail->next = oP;
  outboxTail = oP;
}

static void outboxRecord(const char* endpoint, CorNode* recordP)
{
  Outbox* oP = calloc(1, sizeof(Outbox));

  if (oP == NULL)
    return;

  int size = corJsonFastRenderSize(recordP);

  oP->endpoint = strdup(endpoint);
  oP->json     = malloc(size + 1);

  if ((oP->endpoint == NULL) || (oP->json == NULL))
  {
    free(oP->endpoint);
    free(oP->json);
    free(oP);
    return;
  }

  corJsonFastRender(recordP, oP->json);
  outboxAdd(oP);
}

static void outboxEnded(const char* executionId, bool ok, const char* why)
{
  Outbox* oP = calloc(1, sizeof(Outbox));

  if ((oP == NULL) || ((oP->executionId = strdup(executionId)) == NULL))
  {
    free(oP);
    return;
  }

  oP->ok = ok;
  snprintf(oP->why, sizeof(oP->why), "%s", (why != NULL) ? why : "");
  outboxAdd(oP);
}

static void outboxDeliver(void)
{
  pthread_mutex_lock(&mutex);
  Outbox* listP = outbox;
  outbox     = NULL;
  outboxTail = NULL;
  pthread_mutex_unlock(&mutex);

  while (listP != NULL)
  {
    Outbox* oP = listP;

    listP = oP->next;

    if (oP->json != NULL)
    {
      int r = brokerP->sampleIn("iotalwm2m", oP->endpoint, oP->json, 0);

      COR_V("iotalwm2m: record on %s (%d): %s", oP->endpoint, r, oP->json);
    }
    else if (executeEnded != NULL)
      executeEnded(oP->executionId, oP->ok, oP->why);

    free(oP->endpoint);
    free(oP->json);
    free(oP->executionId);
    free(oP);
  }
}



// -----------------------------------------------------------------------------
//
// endpointOf - the DataChannel of a client: its own name, else the longest prefix; under the mutex
//
static Endpoint* endpointOf(const char* ep)
{
  Endpoint* bestP = NULL;

  for (Endpoint* eP = endpointList; eP != NULL; eP = eP->next)
  {
    if (eP->prefix == false)
    {
      if (strcmp(eP->name, ep) == 0)
        return eP;
    }
    else if ((strncmp(eP->name, ep, strlen(eP->name)) == 0) && ((bestP == NULL) || (strlen(eP->name) > strlen(bestP->name))))
      bestP = eP;
  }

  return bestP;
}



// -----------------------------------------------------------------------------
//
// clientByEp / clientByRegId - under the mutex
//
static Client* clientByEp(const char* ep)
{
  for (Client* cP = clientList; cP != NULL; cP = cP->next)
  {
    if (strcmp(cP->ep, ep) == 0)
      return cP;
  }
  return NULL;
}

static Client* clientByRegId(const char* regId)
{
  for (Client* cP = clientList; cP != NULL; cP = cP->next)
  {
    if (strcmp(cP->regId, regId) == 0)
      return cP;
  }
  return NULL;
}



// -----------------------------------------------------------------------------
//
// clientFree - a client gone: its requests in flight end (an Execute fails); under the mutex
//
static void clientFree(Client* cP, const char* why)
{
  for (Request** rPP = &requestList; *rPP != NULL; )
  {
    Request* rP = *rPP;

    if (strcmp(rP->ep, cP->ep) != 0)
    {
      rPP = &rP->next;
      continue;
    }

    *rPP = rP->next;
    requests -= 1;
    if (rP->kind == RequestExecute)
      outboxEnded(rP->executionId, false, why);
    free(rP->executionId);
    free(rP);
  }

  while (cP->observeList != NULL)
  {
    Observe* oP = cP->observeList;

    cP->observeList = oP->next;
    free(oP);
  }

  free(cP);
}



// -----------------------------------------------------------------------------
//
// send - a datagram to an address
//
static void datagramSend(const unsigned char* dgram, int len, const struct sockaddr_storage* aP, socklen_t aLen)
{
  if (sendto(sock, dgram, len, 0, (const struct sockaddr*) aP, aLen) != len)
    COR_W("iotalwm2m: sendto: %s", strerror(errno));
}



// -----------------------------------------------------------------------------
//
// requestSend - a confirmable request to a client, kept until it is answered; under the mutex.
// payload: text/plain (NULL: none). false: not sent
//
static bool requestSend(Client* cP, RequestKind kind, int code, const char* path, const char* payload, const char* executionId)
{
  if (requests >= REQUESTS_MAX)
  {
    COR_W("iotalwm2m: %d requests in flight - a request to %s refused", REQUESTS_MAX, cP->ep);
    return false;
  }

  Request* rP = calloc(1, sizeof(Request));

  if (rP == NULL)
    return false;

  CoapMessage   m;
  unsigned char observeStore[4];
  unsigned char formatStore[4];
  uint32_t      t = ++nextToken;

  memset(&m, 0, sizeof(m));
  m.type = COAP_CON;
  m.code = code;
  m.mid  = (++nextMid) & 0xFFFF;
  m.tkl  = 8;
  m.token[0] = t >> 24;  m.token[1] = (t >> 16) & 0xFF;  m.token[2] = (t >> 8) & 0xFF;  m.token[3] = t & 0xFF;
  m.token[4] = 'l';      m.token[5] = 'w';               m.token[6] = 'm';               m.token[7] = '2';

  snprintf(rP->path, sizeof(rP->path), "%s", path);
  coapPathAdd(&m, COAP_OPT_URI_PATH, rP->path);

  if (kind == RequestObserve)
    coapOptionUintAdd(&m, COAP_OPT_OBSERVE, 0, observeStore);

  if ((kind == RequestObserve) || (kind == RequestRead))
    coapOptionUintAdd(&m, COAP_OPT_ACCEPT, COAP_FORMAT_TEXT, formatStore);
  else if (payload != NULL)
    coapOptionUintAdd(&m, COAP_OPT_CONTENT_FORMAT, COAP_FORMAT_TEXT, formatStore);

  if (payload != NULL)
  {
    m.payload    = (const unsigned char*) payload;
    m.payloadLen = strlen(payload);
  }

  rP->len = coapBuild(&m, rP->dgram, sizeof(rP->dgram));

  if (rP->len < 0)
  {
    free(rP);
    return false;
  }

  rP->kind        = kind;
  rP->mid         = m.mid;
  memcpy(rP->token, m.token, 8);
  snprintf(rP->ep, sizeof(rP->ep), "%s", cP->ep);
  rP->executionId = (executionId != NULL) ? strdup(executionId) : NULL;
  rP->addr        = cP->addr;
  rP->addrLen     = cP->addrLen;
  rP->sends       = 1;
  rP->nextSend    = now() + ACK_TIMEOUT;

  datagramSend(rP->dgram, rP->len, &rP->addr, rP->addrLen);

  if (kind == RequestObserve)
  {
    Observe* oP = calloc(1, sizeof(Observe));

    if (oP != NULL)
    {
      snprintf(oP->path, sizeof(oP->path), "%s", path);
      memcpy(oP->token, m.token, 8);
      oP->next        = cP->observeList;
      cP->observeList = oP;
    }
  }

  rP->next    = requestList;
  requestList = rP;
  requests   += 1;
  return true;
}



// -----------------------------------------------------------------------------
//
// observeAll - a client's DataChannel's resources observed; under the mutex
//
static void observeAll(Client* cP)
{
  Endpoint* eP = endpointOf(cP->ep);

  for (int ix = 0; (eP != NULL) && (ix < eP->paths); ix++)
  {
    bool observed = false;

    for (Observe* oP = cP->observeList; (oP != NULL) && (observed == false); oP = oP->next)
      observed = (strcmp(oP->path, eP->pathV[ix]) == 0);

    if (observed == false)
      requestSend(cP, RequestObserve, COAP_GET, eP->pathV[ix], NULL, NULL);
  }
}



// -----------------------------------------------------------------------------
//
// valueOf - a text/plain resource value as JSON: a number when it is one, else a string. LwM2M text has no
// types - a boolean is 0 or 1 (the DataChannel's "as": "boolean" makes it one)
//
static CorNode* valueOf(CorAlloc* kaP, const char* name, const char* text)
{
  char*  rest;
  double d = strtod(text, &rest);

  if ((text[0] != 0) && (*rest == 0) && (isfinite(d) != 0) && (text[0] != ' ') && (strncmp(text, "0x", 2) != 0) && (strncmp(text, "0X", 2) != 0))
  {
    if ((strchr(text, '.') == NULL) && (strchr(text, 'e') == NULL) && (strchr(text, 'E') == NULL))
    {
      errno = 0;

      long long i = strtoll(text, &rest, 10);

      if ((*rest == 0) && (errno != ERANGE))
        return corTreeInteger(kaP, name, i);
    }

    return corTreeFloat(kaP, name, d);
  }

  return corTreeString(kaP, name, text);
}



// -----------------------------------------------------------------------------
//
// valueIn - a resource's value from a client (a read's response, a notification); under the mutex
//
static void valueIn(const char* ep, const char* path, const CoapMessage* mP)
{
  long format = coapOptionUint(mP, COAP_OPT_CONTENT_FORMAT);

  if ((format != -1) && (format != COAP_FORMAT_TEXT) && (format != COAP_FORMAT_OLD_TEXT))
  {
    COR_W("iotalwm2m: %s /%s: content format %ld - only text/plain is taken", ep, path, format);
    return;
  }

  Endpoint* eP = endpointOf(ep);

  if (eP == NULL)
    return;

  CorAlloc ka;
  char     buf[4096];
  char     text[1025];

  snprintf(text, sizeof(text), "%.*s", mP->payloadLen, (const char*) mP->payload);
  corAllocBufferInit(&ka, buf, sizeof(buf), 8 * 1024, NULL, "iotalwm2m value");

  CorNode* recordP = corTreeObject(&ka, NULL);

  if (recordP != NULL)
  {
    corTreeChildAdd(recordP, valueOf(&ka, path, text));
    corTreeChildAdd(recordP, corTreeString(&ka, "@deviceId", ep));
    outboxRecord(eP->endpoint, recordP);
  }

  corAllocBufferReset(&ka, false);
}



// -----------------------------------------------------------------------------
//
// reply - an answer to a client's request: piggybacked on the ACK of a CON, a NON to a NON
//
static void reply(const CoapMessage* reqP, int code, const char* location, const struct sockaddr_storage* aP, socklen_t aLen)
{
  CoapMessage   m;
  unsigned char dgram[256];
  char          loc[64];

  memset(&m, 0, sizeof(m));
  m.type = (reqP->type == COAP_CON) ? COAP_ACK : COAP_NON;
  m.code = code;
  m.mid  = (reqP->type == COAP_CON) ? reqP->mid : ((++nextMid) & 0xFFFF);
  m.tkl  = reqP->tkl;
  memcpy(m.token, reqP->token, reqP->tkl);

  if (location != NULL)
  {
    snprintf(loc, sizeof(loc), "%s", location);
    coapPathAdd(&m, COAP_OPT_LOCATION_PATH, loc);
  }

  int len = coapBuild(&m, dgram, sizeof(dgram));

  if (len > 0)
    datagramSend(dgram, len, aP, aLen);
}



// -----------------------------------------------------------------------------
//
// empty - an empty ACK (a separate response's, a notification's) or RST; mid: the message's
//
static void empty(int type, int mid, const struct sockaddr_storage* aP, socklen_t aLen)
{
  unsigned char dgram[4] = { (1 << 6) | (type << 4), 0, (mid >> 8) & 0xFF, mid & 0xFF };

  datagramSend(dgram, 4, aP, aLen);
}



// -----------------------------------------------------------------------------
//
// registration - the Registration interface: register, update, de-register; under the mutex
//
//   POST   /rd?ep=<name>&lt=<lifetime>&lwm2m=<version>&b=<binding>   </1/0>,</3/0>,</3303/0>
//   POST   /rd/<regId>?lt=...
//   DELETE /rd/<regId>
//
static void registration(const CoapMessage* mP, const char* path, const struct sockaddr_storage* aP, socklen_t aLen)
{
  char addr[80];
  char ep[128];
  char lt[32]      = "86400";
  char version[16] = "1.0";
  char binding[16] = "U";

  addrText(aP, addr, sizeof(addr));

  if ((strcmp(path, "rd") == 0) && (mP->code == COAP_POST))
  {
    if ((coapQuery(mP, "ep", ep, sizeof(ep)) == false) || (ep[0] == 0))
    {
      COR_W("iotalwm2m: a registration from %s without an endpoint name (ep) - 4.00", addr);
      reply(mP, COAP_BAD_REQUEST, NULL, aP, aLen);
      return;
    }

    coapQuery(mP, "lt", lt, sizeof(lt));
    coapQuery(mP, "lwm2m", version, sizeof(version));
    coapQuery(mP, "b", binding, sizeof(binding));

    long lifetime = atol(lt);

    if (lifetime <= 0)
      lifetime = 86400;

    //
    // A client registering again replaces its registration - its observations with it
    //
    for (Client** cPP = &clientList; *cPP != NULL; cPP = &(*cPP)->next)
    {
      if (strcmp((*cPP)->ep, ep) == 0)
      {
        Client* oldP = *cPP;

        *cPP = oldP->next;
        clientFree(oldP, "the device registered again");
        break;
      }
    }

    Client* cP = calloc(1, sizeof(Client));

    if (cP == NULL)
      return;

    snprintf(cP->ep, sizeof(cP->ep), "%s", ep);
    snprintf(cP->regId, sizeof(cP->regId), "%x", ++nextRegId);
    cP->addr     = *aP;
    cP->addrLen  = aLen;
    cP->lifetime = lifetime;
    cP->lastSeen = now();
    cP->next     = clientList;
    clientList   = cP;

    char location[32];

    snprintf(location, sizeof(location), "rd/%s", cP->regId);
    reply(mP, COAP_CREATED, location, aP, aLen);
    COR_I("iotalwm2m: %s registered from %s - /%s, lifetime %lds, LwM2M %s, binding %s", ep, addr, location, lifetime, version, binding);

    //
    // The registration as a record - the entity there before a value is
    //
    Endpoint* eP = endpointOf(ep);

    if (eP != NULL)
    {
      CorAlloc ka;
      char     buf[8 * 1024];
      char     objects[2048];

      snprintf(objects, sizeof(objects), "%.*s", mP->payloadLen, (const char*) mP->payload);
      corAllocBufferInit(&ka, buf, sizeof(buf), 16 * 1024, NULL, "iotalwm2m registration");

      CorNode* recordP = corTreeObject(&ka, NULL);

      if (recordP != NULL)
      {
        corTreeChildAdd(recordP, corTreeString(&ka, "@deviceId", ep));
        corTreeChildAdd(recordP, corTreeInteger(&ka, "@lifetime", lifetime));
        corTreeChildAdd(recordP, corTreeString(&ka, "@lwm2m", version));
        corTreeChildAdd(recordP, corTreeString(&ka, "@binding", binding));
        corTreeChildAdd(recordP, corTreeString(&ka, "@objects", objects));
        outboxRecord(eP->endpoint, recordP);
      }

      corAllocBufferReset(&ka, false);
    }
    else
      COR_V("iotalwm2m: %s registered - no DataChannel has it", ep);

    observeAll(cP);
    return;
  }

  if (strncmp(path, "rd/", 3) != 0)
  {
    reply(mP, COAP_NOT_FOUND, NULL, aP, aLen);
    return;
  }

  const char* regId = &path[3];

  if (mP->code == COAP_POST)
  {
    Client* cP = clientByRegId(regId);

    if (cP == NULL)
    {
      reply(mP, COAP_NOT_FOUND, NULL, aP, aLen);   // the client registers again
      return;
    }

    if (coapQuery(mP, "lt", lt, sizeof(lt)) == true)
      cP->lifetime = (atol(lt) > 0) ? atol(lt) : cP->lifetime;

    cP->addr     = *aP;                             // a NAT may have moved it
    cP->addrLen  = aLen;
    cP->lastSeen = now();
    reply(mP, COAP_CHANGED, NULL, aP, aLen);
    COR_V("iotalwm2m: %s updated its registration", cP->ep);
    return;
  }

  if (mP->code == COAP_DELETE)
  {
    for (Client** cPP = &clientList; *cPP != NULL; cPP = &(*cPP)->next)
    {
      if (strcmp((*cPP)->regId, regId) == 0)
      {
        Client* cP = *cPP;

        *cPP = cP->next;
        COR_I("iotalwm2m: %s de-registered", cP->ep);
        clientFree(cP, "the device de-registered");
        reply(mP, COAP_DELETED, NULL, aP, aLen);
        return;
      }
    }

    reply(mP, COAP_NOT_FOUND, NULL, aP, aLen);
    return;
  }

  reply(mP, COAP_METHOD_NOT_ALLOWED, NULL, aP, aLen);
}



// -----------------------------------------------------------------------------
//
// codeText - a response code as c.dd
//
static const char* codeText(int code, char* buf, int size)
{
  snprintf(buf, size, "%d.%02d", (code >> 5) & 7, code & 0x1F);
  return buf;
}



// -----------------------------------------------------------------------------
//
// responseIn - a response to a request of ours (piggybacked or separate); under the mutex
//
static void responseIn(Request* rP, const CoapMessage* mP)
{
  char code[16];
  char why[256];
  bool ok = ((mP->code >> 5) == 2);

  codeText(mP->code, code, sizeof(code));

  switch (rP->kind)
  {
  case RequestObserve:
  case RequestRead:
    if (mP->code == COAP_CONTENT)
      valueIn(rP->ep, rP->path, mP);
    else
    {
      COR_W("iotalwm2m: %s /%s: %s %s", rP->ep, rP->path, (rP->kind == RequestObserve) ? "observe" : "read", code);

      Client* cP = clientByEp(rP->ep);

      for (Observe** oPP = (cP != NULL) ? &cP->observeList : NULL; (oPP != NULL) && (*oPP != NULL); oPP = &(*oPP)->next)
      {
        if (memcmp((*oPP)->token, rP->token, 8) == 0)
        {
          Observe* oP = *oPP;

          *oPP = oP->next;
          free(oP);
          break;
        }
      }
    }
    break;

  case RequestWrite:
    if (ok)
      COR_V("iotalwm2m: %s /%s written (%s)", rP->ep, rP->path, code);
    else
    {
      Client* cP = clientByEp(rP->ep);

      COR_W("iotalwm2m: %s refused the write of /%s (%s) - the resource read back", rP->ep, rP->path, code);
      if (cP != NULL)
        requestSend(cP, RequestRead, COAP_GET, rP->path, NULL, NULL);
    }
    break;

  case RequestExecute:
    snprintf(why, sizeof(why), "the device answered %s to the Execute of /%s", code, rP->path);
    outboxEnded(rP->executionId, ok, ok ? NULL : why);
    break;
  }
}



// -----------------------------------------------------------------------------
//
// requestUnlink - under the mutex
//
static void requestUnlink(Request* rP)
{
  for (Request** rPP = &requestList; *rPP != NULL; rPP = &(*rPP)->next)
  {
    if (*rPP == rP)
    {
      *rPP = rP->next;
      requests -= 1;
      return;
    }
  }
}



// -----------------------------------------------------------------------------
//
// datagramIn - a datagram from a client; under the mutex
//
static void datagramIn(const unsigned char* buf, int len, const struct sockaddr_storage* aP, socklen_t aLen)
{
  CoapMessage m;

  if (coapParse(buf, len, &m) == false)
    return;

  //
  // A request: the Registration interface
  //
  if ((m.code >= 1) && (m.code < 32))
  {
    char path[256];

    coapPath(&m, path, sizeof(path));
    registration(&m, path, aP, aLen);
    return;
  }

  //
  // An empty ACK - a request of ours acknowledged, its response to come; an RST - refused
  //
  if (m.code == COAP_EMPTY)
  {
    for (Request* rP = requestList; rP != NULL; rP = rP->next)
    {
      if (rP->mid != m.mid)
        continue;

      if (m.type == COAP_ACK)
      {
        rP->acked    = true;
        rP->deadline = now() + RESPONSE_WAIT;
      }
      else if (m.type == COAP_RST)
      {
        COR_W("iotalwm2m: %s reset the request for /%s", rP->ep, rP->path);
        if (rP->kind == RequestExecute)
          outboxEnded(rP->executionId, false, "the device reset the Execute");
        requestUnlink(rP);
        free(rP->executionId);
        free(rP);
      }
      break;
    }
    return;
  }

  //
  // A response - to a request in flight, else a notification of an observation
  //
  if (m.type == COAP_CON)
    empty(COAP_ACK, m.mid, aP, aLen);

  for (Request* rP = requestList; rP != NULL; rP = rP->next)
  {
    if ((m.tkl == 8) && (memcmp(rP->token, m.token, 8) == 0))
    {
      requestUnlink(rP);
      responseIn(rP, &m);
      free(rP->executionId);
      free(rP);
      return;
    }
  }

  for (Client* cP = clientList; cP != NULL; cP = cP->next)
  {
    for (Observe* oP = cP->observeList; oP != NULL; oP = oP->next)
    {
      if ((m.tkl == 8) && (memcmp(oP->token, m.token, 8) == 0))
      {
        if (m.code == COAP_CONTENT)
          valueIn(cP->ep, oP->path, &m);
        return;
      }
    }
  }

  if (m.type == COAP_NON)
    empty(COAP_RST, m.mid, aP, aLen);                 // an observation no one has: the client stops it
  else if (m.type == COAP_CON)
    COR_V("iotalwm2m: a response no request has - acknowledged");
}



// -----------------------------------------------------------------------------
//
// timers - retransmissions, responses never sent, registrations lapsed; under the mutex
//
static void timers(void)
{
  double t = now();

  for (Request** rPP = &requestList; *rPP != NULL; )
  {
    Request* rP = *rPP;

    if (rP->acked == false)
    {
      if (t < rP->nextSend)
      {
        rPP = &rP->next;
        continue;
      }

      if (rP->sends < MAX_SENDS)
      {
        datagramSend(rP->dgram, rP->len, &rP->addr, rP->addrLen);
        rP->nextSend = t + ACK_TIMEOUT * (1 << rP->sends);
        rP->sends   += 1;
        rPP = &rP->next;
        continue;
      }
    }
    else if (t < rP->deadline)
    {
      rPP = &rP->next;
      continue;
    }

    COR_W("iotalwm2m: %s did not answer the request for /%s", rP->ep, rP->path);

    *rPP = rP->next;
    requests -= 1;

    if (rP->kind == RequestExecute)
      outboxEnded(rP->executionId, false, "the device did not answer the Execute");

    free(rP->executionId);
    free(rP);
  }

  for (Client** cPP = &clientList; *cPP != NULL; )
  {
    Client* cP = *cPP;

    if (t < cP->lastSeen + cP->lifetime + 2)
    {
      cPP = &cP->next;
      continue;
    }

    COR_I("iotalwm2m: the registration of %s lapsed (lifetime %lds)", cP->ep, cP->lifetime);
    *cPP = cP->next;
    clientFree(cP, "the device's registration lapsed");
  }
}



// -----------------------------------------------------------------------------
//
// serverThread -
//
static void* serverThread(void* unused)
{
  (void) unused;

  unsigned char buf[2048];

  while (stopping == false)
  {
    struct pollfd pfd = { sock, POLLIN, 0 };

    if (poll(&pfd, 1, 50) > 0)
    {
      struct sockaddr_storage addr;
      socklen_t               addrLen = sizeof(addr);
      int                     n       = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr*) &addr, &addrLen);

      if (n > 0)
      {
        pthread_mutex_lock(&mutex);
        datagramIn(buf, n, &addr, addrLen);
        pthread_mutex_unlock(&mutex);
      }
    }

    pthread_mutex_lock(&mutex);
    timers();
    pthread_mutex_unlock(&mutex);

    outboxDeliver();
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// lwm2mServerOpen -
//
int lwm2mServerOpen(unsigned short port, const BridgeBroker* _brokerP)
{
  brokerP = _brokerP;

  struct timespec ts;

  clock_gettime(CLOCK_REALTIME, &ts);
  nextMid   = (unsigned int) (ts.tv_nsec ^ ts.tv_sec) & 0xFFFF;
  nextToken = (uint32_t) (ts.tv_nsec * 2654435761u);

  sock = socket(AF_INET6, SOCK_DGRAM, 0);

  int off = 0;

  if ((sock < 0) || (setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off)) != 0))
  {
    COR_E("iotalwm2m: a UDP socket: %s", strerror(errno));
    if (sock >= 0)
      close(sock);
    sock = -1;
    return BRIDGE_ERR;
  }

  struct sockaddr_in6 a;

  memset(&a, 0, sizeof(a));
  a.sin6_family = AF_INET6;
  a.sin6_port   = htons(port);
  a.sin6_addr   = in6addr_any;

  if (bind(sock, (struct sockaddr*) &a, sizeof(a)) != 0)
  {
    COR_E("iotalwm2m: cannot listen on UDP port %u: %s", port, strerror(errno));
    close(sock);
    sock = -1;
    return BRIDGE_ERR;
  }

  stopping = false;
  if (pthread_create(&thread, NULL, serverThread, NULL) != 0)
  {
    close(sock);
    sock = -1;
    return BRIDGE_ERR;
  }

  COR_I("iotalwm2m: the LwM2M server on UDP port %u", port);
  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// lwm2mServerClose -
//
void lwm2mServerClose(void)
{
  if (sock < 0)
    return;

  stopping = true;
  pthread_join(thread, NULL);
  close(sock);
  sock = -1;

  pthread_mutex_lock(&mutex);

  while (clientList != NULL)
  {
    Client* cP = clientList;

    clientList = cP->next;
    clientFree(cP, "the broker is stopping");
  }

  while (endpointList != NULL)
  {
    Endpoint* eP = endpointList;

    endpointList = eP->next;
    free(eP->endpoint);
    free(eP->name);
    free(eP);
  }

  while (outbox != NULL)                               // the Executes' ends: not reported, the broker is stopping
  {
    Outbox* oP = outbox;

    outbox = oP->next;
    free(oP->endpoint);
    free(oP->json);
    free(oP->executionId);
    free(oP);
  }
  outboxTail = NULL;

  pthread_mutex_unlock(&mutex);
  brokerP = NULL;
}



// -----------------------------------------------------------------------------
//
// pathCheck - "<object>/<instance>/<resource>", each a number; a leading '/' dropped into 'out'
//
static bool pathCheck(const char* path, char* out, int size)
{
  while (*path == '/')
    ++path;

  int segments = 0;

  for (const char* s = path; *s != 0; s++)
  {
    if (*s == '/')
    {
      if ((s == path) || (s[-1] == '/') || (s[1] == 0))
        return false;
      continue;
    }
    if ((*s < '0') || (*s > '9'))
      return false;
    if ((s == path) || (s[-1] == '/'))
      segments += 1;
  }

  if ((segments != 3) || ((int) strlen(path) >= size))
    return false;

  strcpy(out, path);
  return true;
}



// -----------------------------------------------------------------------------
//
// infoRead - a DataChannel's channelInfo: "observe" into the endpoint; false: refused
//
static bool infoRead(const char* endpoint, const char* info, Endpoint* eP)
{
  if (info == NULL)
    return true;

  CorAlloc ka;
  CorJson  cj;
  char     kaBuf[8 * 1024];
  char*    copy = strdup(info);
  bool     ok   = true;

  if (copy == NULL)
    return false;

  corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 16 * 1024, NULL, "iotalwm2m info");
  corJsonCreate(&cj, &ka);

  CorNode* arrayP = corJsonParse(&cj, copy);

  for (CorNode* itemP = ((arrayP != NULL) && (arrayP->type == CorArray)) ? arrayP->value.head : NULL; (itemP != NULL) && ok; itemP = itemP->next)
  {
    CorNode* keyP   = corTreeLookup(itemP, "key");
    CorNode* valueP = corTreeLookup(itemP, "value");

    if ((keyP == NULL) || (keyP->type != CorString) || (valueP == NULL) || (valueP->type != CorString))
    {
      COR_W("iotalwm2m: DataChannel '%s' - a channelInfo item is { \"key\": <string>, \"value\": <string> }", endpoint);
      ok = false;
    }
    else if (strcmp(keyP->value.s, "observe") == 0)
    {
      char list[2048];

      snprintf(list, sizeof(list), "%s", valueP->value.s);

      char* save = NULL;

      for (char* p = strtok_r(list, ", ", &save); (p != NULL) && ok; p = strtok_r(NULL, ", ", &save))
      {
        if (eP->paths == PATHS_MAX)
        {
          COR_W("iotalwm2m: DataChannel '%s' - more than %d resources to observe", endpoint, PATHS_MAX);
          ok = false;
        }
        else if (pathCheck(p, eP->pathV[eP->paths], sizeof(eP->pathV[0])) == false)
        {
          COR_W("iotalwm2m: DataChannel '%s' - observe '%s': a resource is <object>/<instance>/<resource>", endpoint, p);
          ok = false;
        }
        else
          eP->paths += 1;
      }
    }
    else
    {
      COR_W("iotalwm2m: DataChannel '%s' - channelInfo key '%s': observe", endpoint, keyP->value.s);
      ok = false;
    }
  }

  corAllocBufferReset(&ka, false);
  free(copy);
  return ok;
}



// -----------------------------------------------------------------------------
//
// lwm2mEndpointAdd -
//
int lwm2mEndpointAdd(const char* endpoint, const char* info)
{
  int len = strlen(endpoint);

  if ((len == 0) || (len >= 128) || (strchr(endpoint, '/') != NULL) || ((strchr(endpoint, '*') != NULL) && (strchr(endpoint, '*') != &endpoint[len - 1])))
    return BRIDGE_BAD_INPUT;

  Endpoint* eP = calloc(1, sizeof(Endpoint));

  if (eP == NULL)
    return BRIDGE_ERR;

  if (infoRead(endpoint, info, eP) == false)
  {
    free(eP);
    return BRIDGE_BAD_INPUT;
  }

  eP->endpoint = strdup(endpoint);
  eP->name     = strdup(endpoint);
  eP->prefix   = (endpoint[len - 1] == '*');
  if (eP->prefix)
    eP->name[len - 1] = 0;

  pthread_mutex_lock(&mutex);

  eP->next     = endpointList;
  endpointList = eP;

  for (Client* cP = clientList; cP != NULL; cP = cP->next)   // clients already registered: observed now
  {
    if (endpointOf(cP->ep) == eP)
      observeAll(cP);
  }

  pthread_mutex_unlock(&mutex);
  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// lwm2mEndpointDel - the observations stay: their notifications are records no DataChannel has
//
int lwm2mEndpointDel(const char* endpoint)
{
  Endpoint* foundP = NULL;

  pthread_mutex_lock(&mutex);

  for (Endpoint** ePP = &endpointList; *ePP != NULL; ePP = &(*ePP)->next)
  {
    if (strcmp((*ePP)->endpoint, endpoint) == 0)
    {
      foundP = *ePP;
      *ePP   = foundP->next;
      break;
    }
  }

  pthread_mutex_unlock(&mutex);

  if (foundP == NULL)
    return BRIDGE_NOT_FOUND;

  free(foundP->endpoint);
  free(foundP->name);
  free(foundP);
  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// textOf - a JSON value as an LwM2M text/plain value: a boolean 1 or 0; false: not a value of one resource
//
static bool textOf(CorNode* valueP, char* buf, int size)
{
  switch (valueP->type)
  {
  case CorInt:      snprintf(buf, size, "%lld", valueP->value.i);           return true;
  case CorBoolean:  snprintf(buf, size, "%s", valueP->value.b ? "1" : "0");  return true;
  case CorString:   return (snprintf(buf, size, "%s", valueP->value.s) < size);
  case CorFloat:
    for (int digits = 15; digits <= 17; digits++)
    {
      snprintf(buf, size, "%.*g", digits, valueP->value.f);
      if (strtod(buf, NULL) == valueP->value.f)
        break;
    }
    return true;
  default:
    return false;
  }
}



// -----------------------------------------------------------------------------
//
// lwm2mPublish - each of the fragment's resource columns ("3311/0/5850") a Write, text/plain. The device:
// the fragment's @deviceId (the id template's), else the endpoint's own name
//
int lwm2mPublish(const char* endpoint, const char* json)
{
  CorAlloc ka;
  CorJson  cj;
  char     buf[8 * 1024];
  char*    copy = strdup(json);
  int      r    = BRIDGE_OK;

  if (copy == NULL)
    return BRIDGE_ERR;

  corAllocBufferInit(&ka, buf, sizeof(buf), 16 * 1024, NULL, "iotalwm2m write");
  corJsonCreate(&cj, &ka);

  CorNode* fragmentP = corJsonParse(&cj, copy);
  char     ep[128]   = "";

  pthread_mutex_lock(&mutex);

  Endpoint* eP = NULL;

  for (eP = endpointList; (eP != NULL) && (strcmp(eP->endpoint, endpoint) != 0); eP = eP->next)
    ;

  if (eP == NULL)
    r = BRIDGE_NOT_FOUND;
  else if ((fragmentP == NULL) || (fragmentP->type != CorObject))
    r = BRIDGE_BAD_INPUT;
  else
  {
    CorNode* idP = corTreeLookup(fragmentP, "@deviceId");

    if ((idP != NULL) && (idP->type == CorString))
      snprintf(ep, sizeof(ep), "%s", idP->value.s);
    else if (eP->prefix == false)
      snprintf(ep, sizeof(ep), "%s", eP->name);

    if (ep[0] == 0)
    {
      COR_W("iotalwm2m: a write on '%s' refused - which device: no @deviceId in the entity's id template, and the endpoint names none", endpoint);
      r = BRIDGE_BAD_INPUT;
    }
  }

  //
  // Every column checked before any is sent: a write is sent whole or not at all
  //
  char pathV[16][32];
  char textV[16][256];
  int  writes = 0;

  for (CorNode* memberP = (r == BRIDGE_OK) ? fragmentP->value.head : NULL; (memberP != NULL) && (r == BRIDGE_OK); memberP = memberP->next)
  {
    if (memberP->name[0] == '@')
      continue;

    if ((writes == 16) || (pathCheck(memberP->name, pathV[writes], sizeof(pathV[0])) == false))
    {
      COR_W("iotalwm2m: a write of %s refused - '%s' is not a resource, <object>/<instance>/<resource>", ep, memberP->name);
      r = BRIDGE_BAD_INPUT;
    }
    else if (textOf(memberP, textV[writes], sizeof(textV[0])) == false)
    {
      COR_W("iotalwm2m: a write of %s refused - /%s: an object, an array or null is no value of a resource", ep, memberP->name);
      r = BRIDGE_BAD_INPUT;
    }
    else
      writes += 1;
  }

  if (r == BRIDGE_OK)
  {
    Client* cP = clientByEp(ep);

    if (cP == NULL)
    {
      COR_W("iotalwm2m: a write of %s refused - the device is not registered", ep);
      r = BRIDGE_ERR;
    }

    for (int ix = 0; (ix < writes) && (r == BRIDGE_OK); ix++)
    {
      if (requestSend(cP, RequestWrite, COAP_PUT, pathV[ix], textV[ix], NULL) == false)
        r = BRIDGE_ERR;
      else
        COR_V("iotalwm2m: write of %s /%s: %s", ep, pathV[ix], textV[ix]);
    }
  }

  pthread_mutex_unlock(&mutex);
  corAllocBufferReset(&ka, false);
  free(copy);
  return r;
}



// -----------------------------------------------------------------------------
//
// lwm2mExecuteSet -
//
void lwm2mExecuteSet(Lwm2mExecuteEnded ended)
{
  executeEnded = ended;
}



// -----------------------------------------------------------------------------
//
// lwm2mExecute - lwm2m://<endpoint name>/<object>/<instance>/<resource>; the input's "arguments" (a string,
// LwM2M's "0='on',1=30") the Execute's payload - every other member (but the entity's id and type) refused
//
int lwm2mExecute(const char* url, const char* executionId, const char* inputJson)
{
  if (strncmp(url, "lwm2m://", 8) != 0)
    return BRIDGE_BAD_INPUT;

  char        ep[128];
  char        path[32];
  const char* slash = strchr(&url[8], '/');

  if ((slash == NULL) || (slash == &url[8]) || (slash - &url[8] >= (int) sizeof(ep)) || (pathCheck(slash + 1, path, sizeof(path)) == false))
  {
    COR_W("iotalwm2m: service '%s' - lwm2m://<endpoint name>/<object>/<instance>/<resource>", url);
    return BRIDGE_BAD_INPUT;
  }

  snprintf(ep, sizeof(ep), "%.*s", (int) (slash - &url[8]), &url[8]);

  CorAlloc ka;
  CorJson  cj;
  char     buf[8 * 1024];
  char*    copy      = strdup((inputJson != NULL) ? inputJson : "{}");
  char     args[512] = "";
  bool     hasArgs   = false;
  int      r         = BRIDGE_OK;

  if (copy == NULL)
    return BRIDGE_ERR;

  corAllocBufferInit(&ka, buf, sizeof(buf), 16 * 1024, NULL, "iotalwm2m execute");
  corJsonCreate(&cj, &ka);

  CorNode* inputP = corJsonParse(&cj, copy);

  if ((inputP == NULL) || (inputP->type != CorObject))
    r = BRIDGE_BAD_INPUT;

  for (CorNode* memberP = (r == BRIDGE_OK) ? inputP->value.head : NULL; (memberP != NULL) && (r == BRIDGE_OK); memberP = memberP->next)
  {
    if ((strcmp(memberP->name, "id") == 0) || (strcmp(memberP->name, "type") == 0))
      continue;

    if ((strcmp(memberP->name, "arguments") == 0) && (memberP->type == CorString) && (strlen(memberP->value.s) < sizeof(args)))
    {
      snprintf(args, sizeof(args), "%s", memberP->value.s);
      hasArgs = (args[0] != 0);
    }
    else
    {
      COR_W("iotalwm2m: the Execute of %s /%s refused - input member '%s': an Execute takes \"arguments\", a string", ep, path, memberP->name);
      r = BRIDGE_BAD_INPUT;
    }
  }

  if (r == BRIDGE_OK)
  {
    pthread_mutex_lock(&mutex);

    Client* cP = clientByEp(ep);

    if (cP == NULL)
    {
      COR_W("iotalwm2m: the Execute of %s /%s refused - the device is not registered", ep, path);
      r = BRIDGE_ERR;
    }
    else if (requestSend(cP, RequestExecute, COAP_POST, path, hasArgs ? args : NULL, executionId) == false)
      r = BRIDGE_ERR;

    pthread_mutex_unlock(&mutex);
  }

  corAllocBufferReset(&ka, false);
  free(copy);
  return r;
}
