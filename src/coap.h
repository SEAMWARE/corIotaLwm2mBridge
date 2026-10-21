#ifndef LWM2M_COAP_H_
#define LWM2M_COAP_H_

//
// FILE            coap.h
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// CoAP (RFC 7252) messages - what an LwM2M server reads and writes over UDP: the header, the token, the
// options (delta-encoded), the payload. Observe (RFC 7641) is an option like any other. No block-wise
// transfer (RFC 7959): an LwM2M single resource fits a datagram.
//
#include <stdbool.h>                                  // bool



// -----------------------------------------------------------------------------
//
// Message types, codes (class * 32 + detail) and the option numbers an LwM2M server uses
//
#define COAP_CON                 0
#define COAP_NON                 1
#define COAP_ACK                 2
#define COAP_RST                 3

#define COAP_EMPTY               0
#define COAP_GET                 1
#define COAP_POST                2
#define COAP_PUT                 3
#define COAP_DELETE              4

#define COAP_CODE(c, d)          (((c) << 5) | (d))
#define COAP_CREATED             COAP_CODE(2, 1)
#define COAP_DELETED             COAP_CODE(2, 2)
#define COAP_CHANGED             COAP_CODE(2, 4)
#define COAP_CONTENT             COAP_CODE(2, 5)
#define COAP_BAD_REQUEST         COAP_CODE(4, 0)
#define COAP_NOT_FOUND           COAP_CODE(4, 4)
#define COAP_METHOD_NOT_ALLOWED  COAP_CODE(4, 5)

#define COAP_OPT_OBSERVE         6
#define COAP_OPT_LOCATION_PATH   8
#define COAP_OPT_URI_PATH        11
#define COAP_OPT_CONTENT_FORMAT  12
#define COAP_OPT_URI_QUERY       15
#define COAP_OPT_ACCEPT          17

#define COAP_FORMAT_TEXT         0                    // text/plain - an LwM2M single resource
#define COAP_FORMAT_LINK         40                   // application/link-format - a registration's objects
#define COAP_FORMAT_OLD_TEXT     1541                 // LwM2M 1.0 drafts' text (iotagent-lwm2m's writeFormat)

#define COAP_OPTIONS_MAX         32



// -----------------------------------------------------------------------------
//
// CoapOption / CoapMessage - a parsed message points into the datagram it was parsed from
//
typedef struct CoapOption
{
  int                   number;
  const unsigned char*  value;
  int                   len;
} CoapOption;

typedef struct CoapMessage
{
  int                   type;
  int                   code;
  int                   mid;
  unsigned char         token[8];
  int                   tkl;
  CoapOption            optionV[COAP_OPTIONS_MAX];
  int                   options;
  const unsigned char*  payload;
  int                   payloadLen;
} CoapMessage;



// -----------------------------------------------------------------------------
//
// coapParse - a datagram as a message; false: not CoAP
//
extern bool coapParse(const unsigned char* buf, int len, CoapMessage* mP);



// -----------------------------------------------------------------------------
//
// coapBuild - a message as a datagram, its options sorted here; the datagram's length, -1: it does not fit
//
extern int coapBuild(CoapMessage* mP, unsigned char* buf, int size);



// -----------------------------------------------------------------------------
//
// coapOptionAdd / coapOptionUintAdd / coapPathAdd - an option onto a message being built; the value is
// pointed to, not copied. coapPathAdd: "/3303/0/5700" as one Uri-Path option per segment (the text must
// outlive the build)
//
extern bool coapOptionAdd(CoapMessage* mP, int number, const void* value, int len);
extern bool coapOptionUintAdd(CoapMessage* mP, int number, unsigned int value, unsigned char* store);
extern bool coapPathAdd(CoapMessage* mP, int number, const char* path);



// -----------------------------------------------------------------------------
//
// coapOptionUint - an option's value as a number (the first such option); -1: not there
//
extern long coapOptionUint(const CoapMessage* mP, int number);



// -----------------------------------------------------------------------------
//
// coapPath - the message's Uri-Path options as "rd/5a3f" (no leading '/'); coapQuery - a Uri-Query
// option's value, "ep=..." -> its "...", into buf; false: not there
//
extern void coapPath(const CoapMessage* mP, char* buf, int size);
extern bool coapQuery(const CoapMessage* mP, const char* key, char* buf, int size);

#endif  // LWM2M_COAP_H_
