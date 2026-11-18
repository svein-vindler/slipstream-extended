# Explicit calendar continuation

A qualified calendar slice supplies its exact closed week/month periods to the existing bounded refresh. Missing queue rows are activated even when the canonical source is already stored unchanged. A period-name filter alone does not create queue work.

Activation retains the existing queue bound, write allowance, conditional queue writes and physical source/context publication fences. It never republishes raw source or adds rolling endpoints. Existing rolling window selections remain unchanged.

Completed product checkpoints preserve existing product bytes and revisions without repeating product PUTs. Directly requesting the same explicit refresh again can still checkpoint queue housekeeping.
