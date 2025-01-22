# DISTRIBUTED FILE SYSTEM

## `Assumptions`
- Based on provided requirements, `Maximum 12` parallel Processes (includes Metadata Server) can be initialized
- No word in any given file is greater than 32 bytes. `MAX Word Size : 32 bytes`
- Failover detection happens if no HeartBeat received for longer than 3 seconds

## `Features`

### Feature 1
