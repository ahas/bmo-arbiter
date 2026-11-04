# GPU cards

Read what every GPU node's card is doing at the moment the card master last read it: how much of the card
is in use, which service or turn holds it, what engine is resident on it with the memory it takes, and who
waits in its queue. It is the owner's answer to why cards stand idle and whether a line is actually using
them, with no process, pid or person's name in it.

## Arguments

None. Call it with `{}`.

## Where it reads

The card master on the machine's loopback, `http://127.0.0.1:7440` unless the deployment sets
`CARD_MASTER_PORT` or `CARD_MASTER_URL`. The master's token, where it has one, comes from
`CARD_MASTER_TOKEN`. Neither set in the tool's own environment is read from the deployed master's settings,
`~/.config/bmo-arbiter/env`.

When the master does not answer, the call answers with an error saying so and the address it asked. When
the master answers but one of its nodes does not, that node appears in the reading as not reached, with why.

## The reading

The first line counts the cards, how many are in use, how many are idle, how many have waiters, and how
many nodes the master could not reach. Then each card is a line of its own, and under it the engine
resident on the card and the tickets waiting for it:

```
4 cards on 4 nodes: 1 in use, 3 idle, 2 waiting.
n2 card 0 (RTX 4090): 87% used, 21000 of 24564 MiB, 3564 free; turn model (ticket 123) in use
  engine: model/weights 18000 MiB resident; model/store 4608 MiB resident
  waiting: voice ticket 124, position 1, waited 1000 ms, voice/large 6000 MiB
n3 card 0 (RTX 4090): 0% used, 812 of 24564 MiB, 23752 free; no turn
  engine: model/store 4608 MiB in the store
```

- `use unread` stands where the driver gives no use rate.
- A card's `turn` is the turn standing on it, whether or not it is in use; a `claim` names a claimant that
  took the card outright.
- An engine line names every unit on the card with the place it stands in: resident on the card, in host
  memory, in the store, or partly out.

The same reading is in `structuredContent`, with every node, card, turn, claim, unit and waiter as fields,
for a program that reads it.
