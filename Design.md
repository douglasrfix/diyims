# Philosophy

The user is the focus. All references are from the user perspective. PeerID means the user's peers(the NodeId to them) NodeID means the user's NodeID If there is multiple interpretations then it must be matched with a self designator.

The local database is the focus not the network. Peers are sharing transactions to drive changes in the not self nodes. The self node is changing the database which dives these transaction creation.The peer table it self is a composite object. It contains db rows which are created by individual actions that are tracked by the header chain.

Teh entries in the table represent network peer discoveries. These are propagated through the net so it doesn't matter who you register with. The entries contain the original cid

The application should integrate smoothly into the users experience as if it was just another browser application on the platform.

Minimum code development. Don't write it if you can use an already existing product. Think Legos. Function first, cosmetics later, maybe.


The string 'Null' is used to avoid language and utility behaviors. SQLLite, Python, Json have bee tested so far. They all treat it as a string.

Where a name is used in more than one context such as db and python the lowercase underscore convention is used to avoid remembering the names.

rpc-2.0

# Application Structure

1. Externally developed resources

    - Distribution Services
        IPFS
    - Local Database
        SQLite/aiosql
    - HTTP Host
        uvicorn
    - GUI framework
        FastAPI
    - Command line
        Typer

2. Locally developed components

    - GUI
        Console Interface
        Browser Interface

    - Daemon
        Periodically examine Distribution Services for new network peers and reflect additions in DB
        Periodically examine Distribution Services and reflect changes in DB
        Periodically examine the DB and reflect changes in Distribution Services

    - One time
         DB build and initialization
         Application configuration

3. Development phases

    - 0 (0.0.0a0)
        Establish and test development environment
        Create and test minimal CLI application that exercises primary functional components for a single python environment
        Verify behavior after installing the application through normal means on a different platform with same architecture

        python 3.9 min due to debian distribution limitations
