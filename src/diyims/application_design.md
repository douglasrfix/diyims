
Philosophy

Assume a low power dedicated machine for the entire application.
Assume most of the activity is communication based and therefore mostly waiting for a response.
Assume no advance knowledge of existence or location of application participants.
Assume minimal to no technical skills on the part of the user.
Assume end user(GUI) response time is measured at the browser.

Therefore:
    Utilize maximum parallelism at the expense of component efficiency. The goal here is to get as much
        useful activity going at any given time rather than worry about optimizing individual components.
        Get as many components in a wait state as possible. If there isn't something waiting then nothing is ready to happen.

    Parallelism is achieved via long running processes rather than threads. This provides platform and language independence. Coordination is achieved using shared queues. The relevant activities are structured in the fashion of old school input, process and output with the input triggered by a shared queue put. The output is stored in a database along with state information to allow the next task in the process to perform it's activities when notified by a put from the process that completed the current task.

    Given a single piece of shared information the application must be able to bootstrap it's entry into the network and synchronize itself with the rest of the network and begin normal operations. It must be able to recover from network outage on the local host or on the part of a peer. It must cleanup after itself in the course of normal operations with no outside help.






Theory of operation

    The primary functions are; finding new peers, maintaining peers in general, capturing peer activity, publishing local activity and interacting with the user.

    The provider discovery task is responsible for finding new peers which is accomplished by finding the providers of a known IPFS object and storing the peers in the peer table and notifying the provider processing task.

    The provider processing task is responsible for capturing sufficient information to enable the peer maintenance process to expand and update the peer entry as required now and in the future.

        All peers are expected to advertise the cid of it's most current peer table entry which contains an IPNS name that points to the most recent entry of the published item chain. This name is put in the peer entry and the peer maintenance process is notified.

        The process of advertising is accomplished by creating an entry in the peer's wantlist for a period that is long enough to be reasonably unique. After the time period has elapsed, the wantlist item is satisfied ba a local process since there is no option for timeout of the wanted item.

        All peers are expected to look for a peer, new to the local peer, advertising this cid by examining the peer's wantlist and storing the entries in the database. After storing the entries the search for advertised cid process is notified.







Peer states:

    WLR wantlist processing request - set by peer capture process
    WLP wantlist processing pending - set by wantlist processing task manager
    WLX wantlist processing active - set by peer wantlist processing task(s)
    WLC wantlist processing complete - set by peer registration process task(s)
    WLZ want list processing terminated due to excess zero wantlist samples - set primarily by performance
        testing processes.

    NPR normal peer processing request - set by peer processing ? maybe instead of WLC
    NPP normal peer processing pending
    NPX normal peer processing active
    NPC normal peer processing complete



















# TODO: include any peer entries from known nodes
# TODO: find providers
# TODO: for each provider insert in peer_table with New flag
# TODO: for each new provider
# TODO:     generate cid for ipns name ,dts file
#           get the cid(wait)
#
#           for some period(?) look for high count repeat want list items
#           for each entry in providers want list insert record in want_table
#           select from want_table grouped by want cid, order by count in group desc order by dts ascending
#               capture most recent dts and delete entries that ont have a current time stamp to filter out
#           the satisfied wants of the canceled wants. for each batch compare the last seen timestamp to the current time stamp and delete the
#           no longer seen
#
            if the entry reflects self cid than we have a match or
                if the entry reflects the highest count
                    put that cid in the want list

            add ipns name file with hash only No to satisfy the remote and local get do not pin the local want.locals

Theory of operation


add first see to a want_list entry for a peer to allow for calculating residence time. this would allow
the peer to pulse or flash the cid to distinguish it from a cid not being used a beacon. you code send morse code with such a technique. to blink you would have to satisfy the get and therefore use a different cid for each pulse. the puls must last long enough to hang s get and wait for the end of the pulse.  a long short pulse train would for the same peer would be simple to detect.  the response would be a pulse train in response which would allow the two peers to sync up and allow a few pulses for every one to hang a get before you quite the beaconing.
the cid would not be pinned.

could you detect the same behavior in the swarm or bitswap to advantage?

need a function to generate pulses. and detect pulses and one to detect the bit sequence of a network id

this beaconing would be continuous 1 min on 1 off? 2min on 2 off alternating like a light house maybe based on the network name to avoid collisions.

cycle back to the peer table to determine if we need to watch that peer any longer

collect into a buffer and track the current eight bits 0000 1010 0000, 0000 1010 0000
the four bit code in the middle must start with a 1 with three bits for the id maybe the last three bits of the network name? maybe to short causing collisions.

# TODO:
# TODO:     if a remote node want list entry is satisfied, update ent peer_entry table with ipns and cid of peer_table from file
#            add/update entries from the remote table and start over unpin the file cid and the peer table cid


# return the process by extracting the providers again
# TODO:
# TODO:
# TODO:
# TODO:
# TODO:
# TODO:
# TODO:
