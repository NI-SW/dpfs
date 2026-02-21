disk metadata
blockid == [nodeId, 0] -> unused block

system collections metadata
blockid == [nodeId, 16] -> system collections schema
blockid == [nodeId, 20] -> system tables
blockid == [nodeId, 24] -> system columns
blockid == [nodeId, 28] -> system constants
blockid == [nodeId, 32] -> system indexes
|[16,20) systemboot| [20, 24) systab| [24, 28) syscol| [28, 32) sysconst|
|[32, 36) sysindex|