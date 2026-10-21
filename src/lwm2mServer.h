#ifndef LWM2M_SERVER_H_
#define LWM2M_SERVER_H_

//
// FILE            lwm2mServer.h
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// An LwM2M server over CoAP/UDP: the clients' registrations (the Registration interface - POST /rd, its
// updates and de-registrations), the resources a DataChannel observes as its records, a written attribute
// as a Write (PUT) and a Service Execution as an Execute (POST) - the Device Management interface.
//
#include <stdbool.h>                                  // bool

#include "corBridge/BridgeBroker.h"                   // BridgeBroker



// -----------------------------------------------------------------------------
//
// lwm2mServerOpen - the UDP socket on 'port' (every address) and its thread; BRIDGE_OK or BRIDGE_ERR
//
extern int lwm2mServerOpen(unsigned short port, const BridgeBroker* brokerP);



// -----------------------------------------------------------------------------
//
// lwm2mServerClose - the thread stopped: no thread of the plugin calls the broker after it
//
extern void lwm2mServerClose(void);



// -----------------------------------------------------------------------------
//
// lwm2mEndpointAdd / Del - a DataChannel's endpoint: a client's endpoint name, or <prefix>* (every client
// whose name starts with it - "*" every client); info: its channelInfo (JSON text, NULL: none) - "observe",
// the resources observed, "3303/0/5700,3/0/9"
//
extern int lwm2mEndpointAdd(const char* endpoint, const char* info);
extern int lwm2mEndpointDel(const char* endpoint);



// -----------------------------------------------------------------------------
//
// lwm2mPublish - a write-back fragment as Writes, one per resource it has
//
extern int lwm2mPublish(const char* endpoint, const char* json);



// -----------------------------------------------------------------------------
//
// lwm2mExecute - a Service Execution as an Execute: lwm2m://<endpoint name>/<object>/<instance>/<resource>;
// lwm2mExecuteSet - who is told how it ended (the register module's)
//
typedef void (*Lwm2mExecuteEnded)(const char* executionId, bool ok, const char* why);

extern int  lwm2mExecute(const char* url, const char* executionId, const char* inputJson);
extern void lwm2mExecuteSet(Lwm2mExecuteEnded ended);

#endif  // LWM2M_SERVER_H_
