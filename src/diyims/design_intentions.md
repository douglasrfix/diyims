# Distributed Peers

The network of Peers is composed of autonomous systems connect via IPFS.

## Inter Peer Transfer

Objects are materialized with "cat"

## Dealing with immutable objects

Linked list where the newest entry (N) points to the previous entry (N - 1). The newest entry is the published entry.
During installation, a fully populated peer table is entry is created and added to ipfs. The peer table entry has a header table entry pointing to the cid of the added file of the peer table entry. The header table entry is then added to ipfs. As this is the first header table entry, there is no prior entry (N - 1) to point to, thus terminating the list.

## Communicating among peers

Publish a name, resolve a peer, provide a known object, find a provider for a known object, want a recognizable object, examine want list of a peer for a recognizable object.

### Known Object

During the installation process, a dag object delivered with the application is imported. Importing a dag object from a car file guarantees the cid is always the same.
The cid is used by /api/v0/routing/findprovs to discover other ipfs peers with the application installed. As peers are discovered they are stored in the database.

### Recognizable Object

A beacon object is created by using the object cid from the first header table entry in combination with the current date time stamp, guaranteeing a unique cid. This cid is then requested via /api/v0/get which results in a want list entry.
