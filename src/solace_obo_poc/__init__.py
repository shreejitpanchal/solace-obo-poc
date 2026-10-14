"""On-behalf-of (OBO) token propagation through Solace PubSub+ -- POC.

Flow: publisher -> broker (poc/msg/...) -> validator -> broker
(poc/msg-validate/...) -> recipient and user-profiler. Every hop carries the
OBO token as a message user property and authorizes it before reading the
payload.
"""

__version__ = "0.1.0"
