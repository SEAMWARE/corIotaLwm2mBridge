# corIotaLwm2mBridge

**Prototype - not released.** The FIWARE IoT Agent for LwM2M in the coraine NGSI-LD broker: an **LwM2M
server** - its clients' observed resources as **DataChannel records**, a written attribute as a **Write**, and
a Service Execution as an **Execute** - `iotalwm2m.so`, a bridge plugin loaded with `--bridges iotalwm2m`.
CoAP over UDP (RFC 7252, Observe RFC 7641) is the plugin's own: no CoAP library.

## Registrations and observed resources

A client registers with the server (the Registration interface): `POST /rd?ep=<endpoint name>&lt=&lwm2m=&b=`,
its objects in the payload; `POST /rd/<id>` updates the registration, `DELETE /rd/<id>` removes it, and one not
updated within its lifetime lapses. A client registering again replaces its registration.

A DataChannel's endpoint is a client's **endpoint name** - or `<prefix>*`, every client whose name starts with
it (`*` every client); a client's own DataChannel takes it before a prefix does, a longer prefix before a
shorter one.

- **Its resources** are the DataChannel's `channelInfo` `observe`: `"3303/0/5700,3/0/9"` - each observed (GET,
  Observe 0, Accept text/plain) as soon as the client has registered, and again when it registers again.
- **The records**: the observe's response and each notification - `{ "3303/0/5700": 21.5, "@deviceId": "<endpoint name>" }`,
  a column per resource path; the registration itself - `@deviceId`, `@lifetime`, `@lwm2m` (the version),
  `@binding`, `@objects` (its link list), so the entity is there before a value is.
- **A value** is text/plain, which has no types: a number when it reads as one, else a string. An LwM2M
  boolean is 1 or 0 - the attribute's `"as": "boolean"` makes it one.
- A notification no observation has - a client de-registered, a DataChannel removed - is reset (RST): the
  client stops it. A record of a client no DataChannel takes is no record.

## Write-back - Writes

With `"channelDirection": "both"` (coraine's DataChannel write-back, `"idEncoding": "percent"`), a client's
write to a mapped attribute is a Write of its resource - `PUT /<object>/<instance>/<resource>`, text/plain (a
boolean 1 or 0) - one per resource the write has:

- **The device**: the fragment's `@deviceId` (an id template with `{@deviceId}`), else the endpoint's name.
- Refused, nothing stored: a device that is not registered - 503; a column that is not a resource path, a value
  that is an object, an array or null - 400.
- The Write is sent and the client answered at once. A Write the device refuses (4.05 - a read-only resource,
  4.04) was stored: the resource is read back, and the entity shows what the device has.

## Executes - Service Execution

An Execute is a service the bridge executes (coraine `doc/service-execution.md`, bridge ABI 11): a Service
Registration whose endpoint is

    lwm2m://<endpoint name>/<object>/<instance>/<resource>        lwm2m://lamp-1/3/0/4  - the Device object's Reboot

`POST /ngsi-ld/v1/entities/{id}/services/{name}` (or `POST /services`) is then `POST /3/0/4` to the device; the
execution input's `arguments` - a string, LwM2M's `0='fast',1=3` - its payload. The entity's `id` and `type`
are left out; any other member: 400, nothing sent. The device's 2.04 completes the execution (output `{}`); any
other answer, a reset or no answer fails it - 502, the ProblemDetails' title "The LwM2M Execute failed", its
detail why. A device that is not registered: the broker's error (502). An Execute cannot be cancelled.

## Configuration

```json
{
  "iotalwm2m": {
    "server": { "port": 5683 },
    "ngsild": {
      "defaultEntity": false,
      "dataChannels": {
        "lamp-*": { "channelDirection": "both", "idEncoding": "percent",
                    "channelInfo": [ { "key": "observe", "value": "3311/0/5850,3311/0/5851,3/0/9" } ],
                    "entities": [ { "type": "Lamp", "id": "urn:ngsi-ld:Lamp:{@deviceId}",
                                    "attributes": { "on":      { "column": "3311/0/5850", "as": "boolean" },
                                                    "dimmer":  { "column": "3311/0/5851" },
                                                    "battery": { "column": "3/0/9" } } } ] }
      }
    }
  }
}
```

`server` `port`: the UDP port, every address (IPv4 and IPv6) - 5683, CoAP's and iotagent-lwm2m's, when not
said. A channelInfo key other than `observe`, or an `observe` that is not resource paths: the DataChannel is
refused (the broker's log says why).

A Channel or an AssetChannel configured for this bridge is refused (dormant): it carries DataChannels only.

## Not built

DTLS (CoAPs, port 5684) and the Bootstrap interface; the content formats other than text/plain - TLV, SenML
JSON/CBOR, LwM2M JSON - and so reads of a whole object or instance; Write-Attributes (pmin, pmax);
LwM2M 1.1's Send; block-wise transfer; the provisioning API (a DataChannel comes from the configuration
file); iotagent-lwm2m's lazy attributes (a read per query); the server's own retransmission settings
(RFC 7252's defaults: 2 s, 4 retransmissions).

## Build and test

    make && make test          # no transport library; the tests run python3 and curl

`test/funcTests/lwm2mClient.py` plays the clients - CoAP by hand: it registers, answers Reads, Observes, Writes
(a read-only resource refused) and Executes, and notifies when the test changes a resource.
