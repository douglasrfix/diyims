# Security chain

    1. The device executing the ipfs instance is assumed to be operating under the control of the owner at all times.
    2. The peer_ID of the instance is used as a proxy for the owner, meaning there should only be a single peer_ID used for all network activity.
    3. A json file containing only the peer id is signed and the id and signature stored in the peer record.
    4. The peer record is then published via ipns with the id found in the record thus authenticating the publisher as the owner of the peer id.
    5. The peer becomes known to the network by broadcasting a known CID packaged with the application.
    6. The CID is used to identify all providers of that cid and the associated peer_IDs.
    7. The peers are each polled for items on their wantlists that last for a particular duration and and are regular enough to be identifiable.
    8. The identified cid is then examined for the peer record entry which is the source of both the id and signature and the originating peerID.
    9. The peer_id is verified  with the peerID of the provider as well as retrieving the peer record cid and verifying the signature and the peer is made active via the ipns name being used to fetch the published items by that peer.
    10. The items are each unique within the ipfs system by virtue of timing, source  and contents.

## Conclusion

Assuming that the device is secure the verification of each peer allows us to trust our partners as much as any single entity may be trusted.

An asymmetric key is used to prove identity (sign) and protect data (encrypt/decrypt). Keys are managed by the Key API. A key is never stored in plain text, see private key storage for the details.

A key has a local name and a global ID. The ID is the SHA-256 multihash of its public key. The public key is a protobuf encoding containing a type and the DER encoding of the PKCS SubjectPublicKeyInfo.

The key named self is special in that it uniquely identifies the local peer to the IPFS network. It is automatically created with the repository and is controlled by the keychain options.

When initializing an IPFS node, the Peer ID is generated as the SHA-256 multihash of the node's public cryptographic key.

Key Generation: IPFS creates a public/private key pair using asymmetric cryptography, historically RSA (defaulting to 2048-bit keys, prefixed with Qm) or Ed25519 (faster, prefixed with 12D3KooW).
Identity Creation: The system hashes the public key to create the unique Peer ID, which serves as the node's permanent, location-agnostic identity on the distributed web.
Storage: The private key is stored securely in the local configuration (e.g., ~/.ipfs/config), while the Peer ID is derived from the public key at runtime and cannot be changed without regenerating the entire key pair.

Verifying that a specific peer ID is owned by the entity claiming it is not a single-step process within IPFS itself, as peer IDs are cryptographic hashes of public keys rather than ownership certificates. To verify ownership, you must perform a cryptographic signature verification using the public key associated with that peer ID.

The process involves the following steps:

Obtain the Public Key: You need the full public key corresponding to the peer ID. For ed25519 keys (the current default in IPFS), the public key is embedded directly in the peer ID, making verification straightforward.  For RSA keys, the public key is too large to embed, so it must be obtained from an external source (like a DNS record or a separate signature file) and its hash must match the peer ID.
Sign Data with Private Key: The claimant uses their private key to sign a specific piece of data (such as a challenge message or a CID).
Verify Signature: You use the public key (derived from or associated with the peer ID) to verify the signature.  If the signature is valid, it proves that the holder of the private key corresponding to that public key signed the data.


The ipfs key sign command is an experimental feature designed to generate a cryptographic signature for arbitrary data using a specified libp2p key.  It is primarily used to prove ownership of a Peer ID or an IPNS Name.

The command outputs the generated signature in multibase-encoded format.  To ensure clarity and prevent data mangling during transmission, the output is typically structured in JSON.

The JSON output includes the following fields:

Key: The name or CID of the key used for signing (e.g., cidv1-libp2p-key). !!!! standardize in source documentation
Signature: The multibase64url-encoded signature of the provided data.
Err: A null value if successful, or a string describing any error that occurred.

The output of the ipfs key sign command is JSON formatted data containing the signature and the key identifier.

The specific JSON structure includes:

Key: The CIDv1-libp2p-key identifier of the key used for signing.
Signature: The multibase64url-encoded signature of the provided data.
Err: A null or string error message if the signing process fails (e.g., key not found).
Example output:

{
  "Key": "cidv1-libp2p-key",
  "Signature": "[multibase64url-encoded-signature]",
  "Err": null
}

To use this signature for verification, the output is typically piped to jq to extract the signature string, which is then passed to the ipfs key verify command. The signed payload is automatically prefixed with "libp2p-key signed message:" to prevent signature reuse.
