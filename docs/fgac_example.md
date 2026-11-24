## Product scope for agents

Every node written from a product's tables carries exactly one product label (`Support`,
`Sales`, `Marketing`, `ContactCentre`, `CDP`, `WebTracking`). Setup and catalog nodes
(Tenant, User, Team, Channel, Product, ...) carry none.

**Rule:** an agent may read a node when it has no product label, or when its product label
is one of the agent's products. Every node on a path is checked on its own:

```
Support agent (products: Support, CDP)
(Ticket :Support) -ASSIGNED_TO-> (SupportAgent :Support) <-ACTS_AS- (User)          readable
                                                          (User) -ACTS_AS-> (MarketingAgent :Marketing)  stops here
```

It is enforced in our read path, not with database grants:

```python
from src.scope import scoped_read
rows, dropped = scoped_read(driver, "org1012", ["Support", "CDP"],
                            "MATCH (t:Ticket {id: $id})-[*1..2]-(x) RETURN t, x", {"id": 10})
```

`scoped_read` runs the query in a read-only transaction and drops every record that touches
a node of a product the agent may not read. Queries should still name the labels they need.
