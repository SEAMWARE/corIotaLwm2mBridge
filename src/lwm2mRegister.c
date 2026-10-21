//
// FILE            lwm2mRegister.c
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
// The iotalwm2m bridge - the SEAM: what the broker calls, and the one symbol it looks for (bridgeRegister).
// The contract is corBridge's BridgeDriver.h (broker -> plugin) and BridgeBroker.h (plugin -> broker).
//
// The IoT Agent for LwM2M: an LwM2M server - its clients' observed resources as DataChannel records (a
// DataChannel's endpoint is a client's endpoint name, or <prefix>*), with "channelDirection": "both" a
// written attribute a Write, and a Service Execution an Execute (lwm2m://<endpoint name>/<o>/<i>/<r>) -
// lwm2mServer.c. A Channel or an AssetChannel is refused - this bridge carries DataChannels only.
//
#include <stdbool.h>                                  // bool
#include <stdio.h>                                    // fopen, fread, fseek, ftell, snprintf
#include <stdlib.h>                                   // malloc, free

#include "corLog/corLog.h"                            // COR_E, COR_W
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corAlloc/corAllocBufferInit.h"              // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"             // corAllocBufferReset
#include "corJson/CorJson.h"                          // CorJson
#include "corJson/corJsonCreate.h"                    // corJsonCreate
#include "corJson/corJsonParse.h"                     // corJsonParse
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeLookup.h"                    // corTreeLookup

#include "corBridge/BridgeDriver.h"                   // BridgeDriver, BridgeChannelKind, BridgeDirection
#include "corBridge/BridgeBroker.h"                   // BridgeBroker, BRIDGE_*

#include "corJson/corJsonRender.h"                    // corJsonFastRender
#include "corJson/corJsonRenderSize.h"                // corJsonFastRenderSize
#include "corTree/corTreeBuilder.h"                   // corTreeObject, corTreeString, corTreeInteger, corTreeChildAdd

#include "lwm2mServer.h"                              // lwm2mServer*, lwm2mEndpoint*, lwm2mPublish, lwm2mExecute



static const BridgeBroker* hostP = NULL;



// -----------------------------------------------------------------------------
//
// configLoad - the whole --bridgeConfig file, NUL-terminated; malloc'd. NULL: not readable
//
static char* configLoad(const char* path)
{
  FILE* fP = fopen(path, "r");
  if (fP == NULL)
    return NULL;

  char* text = NULL;
  long  size = (fseek(fP, 0, SEEK_END) == 0) ? ftell(fP) : -1;

  if ((size >= 0) && (fseek(fP, 0, SEEK_SET) == 0) && ((text = malloc(size + 1)) != NULL))
  {
    if (fread(text, 1, size, fP) == (size_t) size)
      text[size] = 0;
    else
    {
      free(text);
      text = NULL;
    }
  }

  fclose(fP);
  return text;
}



// -----------------------------------------------------------------------------
//
// iotalwm2mInit - the LwM2M server, from the --bridgeConfig file's member of this plugin:
//
//   { "iotalwm2m": { "server": { "port": 5683 }, "ngsild": { "dataChannels": { "*": { ... } } } } }
//
// No "server", or no "port" in it: 5683, CoAP's (and iotagent-lwm2m's).
//
static int iotalwm2mInit(const char* configFile, const BridgeBroker* brokerP)
{
  CorAlloc   ka;
  CorJson    cj;
  char       buf[16 * 1024];
  char*      text = (configFile != NULL) ? configLoad(configFile) : NULL;
  long long  port = 5683;
  int        r    = BRIDGE_OK;

  corAllocBufferInit(&ka, buf, sizeof(buf), 16 * 1024, NULL, "iotalwm2m config");
  corJsonCreate(&cj, &ka);

  if (text != NULL)
  {
    CorNode* rootP   = corJsonParse(&cj, text);       // valid JSON: the broker has parsed the same file
    CorNode* bridgeP = (rootP != NULL) ? corTreeLookup(rootP, "iotalwm2m") : NULL;
    CorNode* serverP = (bridgeP != NULL) ? corTreeLookup(bridgeP, "server") : NULL;
    CorNode* portP   = (serverP != NULL) ? corTreeLookup(serverP, "port") : NULL;

    if (portP != NULL)
    {
      if ((portP->type != CorInt) || (portP->value.i < 1) || (portP->value.i > 65535))
      {
        COR_E("iotalwm2m: server port - a UDP port number, 1-65535");
        r = BRIDGE_ERR;
      }
      else
        port = portP->value.i;
    }
  }

  hostP = brokerP;

  if (r == BRIDGE_OK)
    r = lwm2mServerOpen((unsigned short) port, brokerP);

  corAllocBufferReset(&ka, false);                    // nothing parsed here is kept
  free(text);
  return r;
}



// -----------------------------------------------------------------------------
//
// iotalwm2mClose - must not return until no thread of the plugin can call the broker
//
static void iotalwm2mClose(void)
{
  lwm2mServerClose();
}



#if BRIDGE_ABI_VERSION >= 11
// -----------------------------------------------------------------------------
//
// executeEnded - an Execute has ended: the Service Execution completed (no output) or failed (why, in a
// ProblemDetails). On the server's thread
//
static void executeEnded(const char* executionId, bool ok, const char* why)
{
  if ((hostP == NULL) || (hostP->abiVersion < 11) || (hostP->serviceUpdateIn == NULL))
    return;

  int r;

  if (ok == true)
    r = hostP->serviceUpdateIn("iotalwm2m", executionId, "completed", NULL, "{}", NULL);
  else
  {
    CorAlloc ka;
    char     kaBuf[4096];
    char     pd[2048];

    corAllocBufferInit(&ka, kaBuf, sizeof(kaBuf), 4096, NULL, "iotalwm2m problem");

    CorNode* pdP = corTreeObject(&ka, NULL);

    corTreeChildAdd(pdP, corTreeString(&ka, "type",   "https://uri.etsi.org/ngsi-ld/errors/InternalError"));
    corTreeChildAdd(pdP, corTreeString(&ka, "title",  "The LwM2M Execute failed"));
    corTreeChildAdd(pdP, corTreeInteger(&ka, "status", 502));
    corTreeChildAdd(pdP, corTreeString(&ka, "detail", (char*) why));

    bool fits = (corJsonFastRenderSize(pdP) < (int) sizeof(pd));

    if (fits == true)
      corJsonFastRender(pdP, pd);

    r = hostP->serviceUpdateIn("iotalwm2m", executionId, "failed", NULL, NULL, (fits == true) ? pd : NULL);
    corAllocBufferReset(&ka, false);
  }

  if ((r != BRIDGE_OK) && (r != BRIDGE_NOT_FOUND))
    COR_W("iotalwm2m: the broker did not take the end of execution '%s' (%d)", executionId, r);
}



// -----------------------------------------------------------------------------
//
// iotalwm2mServiceExecute / Cancel - BridgeDriver.serviceExecute, ABI 11: an Execute; it cannot be taken back
//
static int iotalwm2mServiceExecute(const char* url, const char* executionId, const char* inputJson)
{
  return lwm2mExecute(url, executionId, inputJson);
}

static int iotalwm2mServiceCancel(const char* url, const char* executionId)
{
  (void) url;
  (void) executionId;

  return BRIDGE_UNSUPPORTED;
}
#endif



// -----------------------------------------------------------------------------
//
// The DataChannels - each a client's endpoint name, or a prefix of names. A broker of ABI 15 says which channel type
// an endpoint is (channelTypedAdd); an older one hands every channel over alike (channelAddInfo), and each
// is taken as a DataChannel.
//
static int iotalwm2mChannelAddInfo(const char* endpoint, BridgeChannelKind kind, BridgeDirection direction, const char* info)
{
  (void) kind;
  (void) direction;                                   // in: the observed resources; both: also Writes

  int r = lwm2mEndpointAdd(endpoint, info);

  if (r == BRIDGE_BAD_INPUT)
    COR_W("iotalwm2m: DataChannel '%s' refused - its endpoint is an endpoint name or <prefix>*, its channelInfo observe", endpoint);

  return r;
}

#if BRIDGE_ABI_VERSION >= 15
static int iotalwm2mChannelTypedAdd(const char* endpoint, BridgeChannelType type, BridgeChannelKind kind, BridgeDirection direction, const char* info)
{
  if (type == BridgeChannelTypeDataChannel)
    return iotalwm2mChannelAddInfo(endpoint, kind, direction, info);

  const char* reason = "the iotalwm2m bridge carries DataChannels only - a resource's value is a record";

  if ((hostP != NULL) && (hostP->abiVersion >= 12) && (hostP->channelStatusIn != NULL))
    hostP->channelStatusIn("iotalwm2m", endpoint, BridgeChannelDormant, reason);

  COR_W("iotalwm2m: '%s' refused - %s", endpoint, reason);
  return BRIDGE_UNSUPPORTED;
}
#endif

static int iotalwm2mChannelAdd(const char* endpoint, BridgeChannelKind kind, BridgeDirection direction)
{
  return iotalwm2mChannelAddInfo(endpoint, kind, direction, NULL);
}

static int iotalwm2mChannelDel(const char* endpoint)
{
  return lwm2mEndpointDel(endpoint);
}

static int iotalwm2mPublish(const char* endpoint, const char* json)
{
  return lwm2mPublish(endpoint, json);
}



// -----------------------------------------------------------------------------
//
// iotalwm2mVersionInfo - one line for GET /version
//
static const char* iotalwm2mVersionInfo(void)
{
  return "iotalwm2m 0.1.0";
}



// -----------------------------------------------------------------------------
//
// bridgeRegister - the one symbol the broker looks for, and the one this .so exports
//
// A slot is filled only when the broker's abiVersion has it; abiVersion is then overwritten with this
// plugin's. Which channel type an endpoint is, ABI 15: before it, every channel is taken as a DataChannel.
// The Executes, ABI 11.
//
__attribute__((visibility("default"))) void bridgeRegister(BridgeDriver* driverP)
{
  if (driverP->abiVersion < 1)
    driverP->abiVersion = 1;                          // a host from before the handshake

  driverP->alias       = "iotalwm2m";                // --bridges iotalwm2m, and its member of --bridgeConfig
  driverP->version     = "0.1.0";
  driverP->args        = NULL;
  driverP->init        = iotalwm2mInit;
  driverP->close       = iotalwm2mClose;
  driverP->versionInfo = iotalwm2mVersionInfo;
  driverP->channelAdd  = iotalwm2mChannelAdd;
  driverP->channelDel  = iotalwm2mChannelDel;
  driverP->publish     = iotalwm2mPublish;

  if (driverP->abiVersion >= 9)
    driverP->channelAddInfo = iotalwm2mChannelAddInfo;

#if BRIDGE_ABI_VERSION >= 11
  if (driverP->abiVersion >= 11)
  {
    driverP->serviceSchemes = "lwm2m";                // lwm2m://<endpoint name>/<object>/<instance>/<resource> - an Execute
    driverP->serviceExecute = iotalwm2mServiceExecute;
    driverP->serviceCancel  = iotalwm2mServiceCancel;
    lwm2mExecuteSet(executeEnded);
  }
#endif

#if BRIDGE_ABI_VERSION >= 15
  if (driverP->abiVersion >= 15)
    driverP->channelTypedAdd = iotalwm2mChannelTypedAdd;
#endif

  driverP->abiVersion  = BRIDGE_ABI_VERSION;
}
