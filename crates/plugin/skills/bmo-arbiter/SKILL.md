---
name: bmo-arbiter
description: Share a GPU card with every other process that uses it, through the bmo-arbiter MCP server. Use before putting anything on a GPU card of a machine the card arbiter stands on (loading a model, a training run, a benchmark), to take a turn on the card and give it back; also to read the card's queue and units, read the cluster's nodes and placements, or put a service on a node or take it off one.
---

# bmo-arbiter

On a machine whose GPU card several services share, a node arbiter keeps one lock on the card and a queue
of tickets for it: whoever is to put anything on the card asks for a turn first, waits its place in line,
uses the card while the turn stands and gives the ticket back once the card is empty again. A master, one
for the cluster, pictures every node's card and places services on nodes. The `bmo-arbiter` MCP server
reaches both, one tool for each route, so a harness can take its turn on a card with nothing else running.

## Starting the server

Register the server with the harness as a stdio MCP server:

```
bmo-arbiter mcp
```

The node's tools reach the node arbiter on this machine's loopback, `http://127.0.0.1:7439` unless
`--card <url>` or `CARD_TURN_PORT` says otherwise; they take no token. The master's tools reach
`http://127.0.0.1:7440` unless `--master <url>` or `CARD_MASTER_PORT` says otherwise, with the master's
token from `--token` or `CARD_MASTER_TOKEN` where the master is configured with one.

## Taking a turn on the card

1. `arbiter_ticket_take` with `{ "body": { "serviceName": "<your service>", "waitMs": 20000 } }`. The
   service name must have a place among the node's occupants; `arbiter_card` lists them in `occupants`.
   Add `waitedMs` where the work has already waited, `givesWayMs` where you can let go of the card in the
   middle of your work and how long that takes, and `need` (`gpus`, `vramMiB`, `unitName`) to show what
   you will use.
2. The answer is the ticket. `turn: true` means the card is yours now. Otherwise `position` and `waitMs` say
   where you stand and how long the wait is counted at: ask `arbiter_ticket` with
   `{ "ticketId": <id>, "waitMs": 20000 }` again and again, each answer held open until your turn comes or
   the time passes. A ticket stands, however long you stay silent, until you give it back.
3. Use the card only while your turn stands. Where you asked with `givesWayMs`, keep asking
   `arbiter_ticket` with `waitMs` while you hold the card: once `giveWay` is true, let go at your next safe
   point; `giveWayFor` names who asked.
4. Before your next piece of work, `arbiter_ticket` with `nextWaitedMs`, how long it has waited: `keep`
   true means carry on with the card, false means let go and ask for a new ticket.
5. Once everything of yours is off the card, `arbiter_ticket_return` with `{ "ticketId": <id> }`. The next
   in line has the card at once.

A process that puts something on the card also takes the card lock the node names (`~/.gpu.lock` unless
the node says otherwise) once its turn is given, and names its service in `BMO_CARD_SERVICE`; the
arbiter's `docs/node-arbiter.md` states the whole contract.

## Reading the card and the cluster

- `arbiter_card`: the card's occupants, ranks and tiers, the claim standing, who holds the lock, every ticket in
  line, how long each service typically holds the card, its units and where each one is (`resident` on the
  card, `ram` or `evicted` out of it), the driver's memory figures and the dump store.
- `arbiter_cluster`: every node the master reads, each card with its units, turn and queue, the groups the
  rules choose, the services the master places with the nodes each stands on now, the demands the master
  holds and its last notes.

## Placing a service

`arbiter_placement_request` with `{ "body": { "kind": "place", "serviceName": "<service>", "nodeName":
"<node>" } }` (or `"kind": "remove"`) asks the master; the answer is the placement, `queued`. Follow it with
`arbiter_placement` and its `placementId` until `state` is `done` or `failed`: `lines` say what the master
did, and `failure` why it stopped.

## When a tool answers an error

A tool error's text is JSON: `status`, `code` and `error`, the arbiter's English sentence. Match on `code`:

- `no-place`: the service has no place among the node's occupants; it cannot take a turn there.
- `not-found`: the ticket is no longer held (ask for a new one), or no placement of that id is kept.
- `conflict`: the change is not allowed in the member's state; the sentence says why.
- `unauthorized`: the master's token is missing or wrong.
- `bad-request`: the arguments are not the shape the tool takes; the sentence names what is wrong.

An error with no `code` means the program could not be reached at all; on a machine with no node arbiter,
nobody else shares the card through it, and the card is taken as it comes.

The routes behind each tool are stated in the arbiter's `docs/api.md` and `docs/openapi.json`, whose
`x-tools` lists every tool's input and output.
