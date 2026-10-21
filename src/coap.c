//
// FILE            coap.c
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
// CoAP messages - see header.
//
#include <stdbool.h>                                  // bool
#include <stdio.h>                                    // snprintf
#include <string.h>                                   // memcpy, strlen, strncmp, strchr

#include "coap.h"                                     // Own interface



// -----------------------------------------------------------------------------
//
// extended - an option's delta or length nibble, with its extension bytes; -1: malformed
//
static int extended(int nibble, const unsigned char** pP, const unsigned char* end)
{
  if (nibble < 13)
    return nibble;

  if (nibble == 13)
  {
    if (*pP + 1 > end)
      return -1;
    return 13 + *(*pP)++;
  }

  if (nibble == 14)
  {
    if (*pP + 2 > end)
      return -1;

    int v = ((*pP)[0] << 8) | (*pP)[1];

    *pP += 2;
    return 269 + v;
  }

  return -1;                                          // 15: the payload marker's, never a delta or a length
}



// -----------------------------------------------------------------------------
//
// coapParse -
//
bool coapParse(const unsigned char* buf, int len, CoapMessage* mP)
{
  memset(mP, 0, sizeof(CoapMessage));

  if ((len < 4) || ((buf[0] >> 6) != 1))
    return false;

  mP->type = (buf[0] >> 4) & 3;
  mP->tkl  = buf[0] & 0xF;
  mP->code = buf[1];
  mP->mid  = (buf[2] << 8) | buf[3];

  if ((mP->tkl > 8) || (4 + mP->tkl > len))
    return false;

  memcpy(mP->token, &buf[4], mP->tkl);

  const unsigned char* p      = &buf[4 + mP->tkl];
  const unsigned char* end    = &buf[len];
  int                  number = 0;

  while (p < end)
  {
    if (*p == 0xFF)
    {
      ++p;
      if (p == end)
        return false;                                 // a marker with no payload
      mP->payload    = p;
      mP->payloadLen = end - p;
      break;
    }

    int delta  = *p >> 4;
    int length = *p & 0xF;

    ++p;
    if (((delta = extended(delta, &p, end)) < 0) || ((length = extended(length, &p, end)) < 0) || (p + length > end))
      return false;

    number += delta;

    if (mP->options < COAP_OPTIONS_MAX)
    {
      mP->optionV[mP->options].number = number;
      mP->optionV[mP->options].value  = p;
      mP->optionV[mP->options].len    = length;
      mP->options += 1;
    }

    p += length;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// nibble - an option's delta or length as its nibble and extension bytes
//
static int nibble(int v, unsigned char* ext, int* extLenP)
{
  if (v < 13)
  {
    *extLenP = 0;
    return v;
  }

  if (v < 269)
  {
    ext[0]   = v - 13;
    *extLenP = 1;
    return 13;
  }

  ext[0]   = (v - 269) >> 8;
  ext[1]   = (v - 269) & 0xFF;
  *extLenP = 2;
  return 14;
}



// -----------------------------------------------------------------------------
//
// coapBuild -
//
int coapBuild(CoapMessage* mP, unsigned char* buf, int size)
{
  //
  // The options by number - a stable sort: the Uri-Path segments keep their order
  //
  for (int i = 1; i < mP->options; i++)
  {
    CoapOption o = mP->optionV[i];
    int        j = i - 1;

    for (; (j >= 0) && (mP->optionV[j].number > o.number); j--)
      mP->optionV[j + 1] = mP->optionV[j];
    mP->optionV[j + 1] = o;
  }

  if (size < 4 + mP->tkl)
    return -1;

  buf[0] = (1 << 6) | ((mP->type & 3) << 4) | (mP->tkl & 0xF);
  buf[1] = mP->code;
  buf[2] = (mP->mid >> 8) & 0xFF;
  buf[3] = mP->mid & 0xFF;
  memcpy(&buf[4], mP->token, mP->tkl);

  int at   = 4 + mP->tkl;
  int prev = 0;

  for (int ix = 0; ix < mP->options; ix++)
  {
    unsigned char dExt[2];
    unsigned char lExt[2];
    int           dLen;
    int           lLen;
    int           d = nibble(mP->optionV[ix].number - prev, dExt, &dLen);
    int           l = nibble(mP->optionV[ix].len, lExt, &lLen);

    if (at + 1 + dLen + lLen + mP->optionV[ix].len > size)
      return -1;

    buf[at++] = (d << 4) | l;
    memcpy(&buf[at], dExt, dLen);
    at += dLen;
    memcpy(&buf[at], lExt, lLen);
    at += lLen;
    memcpy(&buf[at], mP->optionV[ix].value, mP->optionV[ix].len);
    at += mP->optionV[ix].len;

    prev = mP->optionV[ix].number;
  }

  if (mP->payloadLen > 0)
  {
    if (at + 1 + mP->payloadLen > size)
      return -1;

    buf[at++] = 0xFF;
    memcpy(&buf[at], mP->payload, mP->payloadLen);
    at += mP->payloadLen;
  }

  return at;
}



// -----------------------------------------------------------------------------
//
// coapOptionAdd -
//
bool coapOptionAdd(CoapMessage* mP, int number, const void* value, int len)
{
  if (mP->options == COAP_OPTIONS_MAX)
    return false;

  mP->optionV[mP->options].number = number;
  mP->optionV[mP->options].value  = value;
  mP->optionV[mP->options].len    = len;
  mP->options += 1;
  return true;
}



// -----------------------------------------------------------------------------
//
// coapOptionUintAdd - 'store' holds the value's bytes (4) until the build
//
bool coapOptionUintAdd(CoapMessage* mP, int number, unsigned int value, unsigned char* store)
{
  int len = 0;

  if (value > 0xFFFFFF)  store[len++] = value >> 24;
  if (value > 0xFFFF)    store[len++] = (value >> 16) & 0xFF;
  if (value > 0xFF)      store[len++] = (value >> 8) & 0xFF;
  if (value > 0)         store[len++] = value & 0xFF;     // 0: no bytes at all

  return coapOptionAdd(mP, number, store, len);
}



// -----------------------------------------------------------------------------
//
// coapPathAdd -
//
bool coapPathAdd(CoapMessage* mP, int number, const char* path)
{
  while (*path == '/')
    ++path;

  while (*path != 0)
  {
    const char* slash = strchr(path, '/');
    int         len   = (slash != NULL) ? (int) (slash - path) : (int) strlen(path);

    if (coapOptionAdd(mP, number, path, len) == false)
      return false;

    path += len;
    while (*path == '/')
      ++path;
  }

  return true;
}



// -----------------------------------------------------------------------------
//
// coapOptionUint -
//
long coapOptionUint(const CoapMessage* mP, int number)
{
  for (int ix = 0; ix < mP->options; ix++)
  {
    if (mP->optionV[ix].number != number)
      continue;

    long v = 0;

    for (int b = 0; (b < mP->optionV[ix].len) && (b < 4); b++)
      v = (v << 8) | mP->optionV[ix].value[b];

    return v;
  }

  return -1;
}



// -----------------------------------------------------------------------------
//
// coapPath -
//
void coapPath(const CoapMessage* mP, char* buf, int size)
{
  int at = 0;

  buf[0] = 0;

  for (int ix = 0; ix < mP->options; ix++)
  {
    if (mP->optionV[ix].number != COAP_OPT_URI_PATH)
      continue;

    int n = snprintf(&buf[at], size - at, "%s%.*s", (at == 0) ? "" : "/", mP->optionV[ix].len, (const char*) mP->optionV[ix].value);

    if ((n < 0) || (at + n >= size))
      return;
    at += n;
  }
}



// -----------------------------------------------------------------------------
//
// coapQuery -
//
bool coapQuery(const CoapMessage* mP, const char* key, char* buf, int size)
{
  int keyLen = strlen(key);

  for (int ix = 0; ix < mP->options; ix++)
  {
    const CoapOption* oP = &mP->optionV[ix];

    if ((oP->number != COAP_OPT_URI_QUERY) || (oP->len <= keyLen) || (oP->value[keyLen] != '=') || (strncmp((const char*) oP->value, key, keyLen) != 0))
      continue;

    snprintf(buf, size, "%.*s", oP->len - keyLen - 1, (const char*) &oP->value[keyLen + 1]);
    return true;
  }

  return false;
}
